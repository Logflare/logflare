use rustler::types::list::ListIterator;
use rustler::types::map::MapIterator;
use rustler::{Binary, Encoder, Env, Term};

use crate::coerce;
use crate::mapping::{
    CompiledField, CompiledMapping, Enum8Data, FieldType, PathSource, Predicate, PredicateValue,
};
use crate::query;
use crate::string_filters;

use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

mod atoms {
    rustler::atoms! {
        nil,
    }
}

/// Check if a field type is an array type.
fn is_array_type(field_type: &FieldType) -> bool {
    matches!(
        field_type,
        FieldType::ArrayString
            | FieldType::ArrayUInt64
            | FieldType::ArrayFloat64
            | FieldType::ArrayDateTime64 { .. }
            | FieldType::ArrayJson
            | FieldType::ArrayMap
            | FieldType::ArrayFlatMap
    )
}

/// Execute the mapping on a single document, returning the mapped output map.
///
/// When `flat_keys` is true, dotted paths are resolved as literal flat-key
/// lookups on the input map instead of nested map navigation.
pub fn map_single<'a>(
    env: Env<'a>,
    body: Term<'a>,
    mapping: &CompiledMapping,
    flat_keys: bool,
) -> Term<'a> {
    let nil = atoms::nil().encode(env);
    let field_count = mapping.fields.len();

    let mut keys: Vec<Term<'a>> = Vec::with_capacity(field_count);
    let mut values: Vec<Term<'a>> = Vec::with_capacity(field_count);
    let mut query_cache =
        query::QueryCache::new(mapping.path_cache_size, mapping.root_cache_size, nil);
    if !flat_keys
        && !mapping.root_cache_keys.is_empty()
        && body.map_size().unwrap_or(usize::MAX) <= mapping.root_cache_scan_limit
    {
        query_cache.preload_root(body, &mapping.root_cache_keys);
    }

    for field in &mapping.fields {
        let is_array = is_array_type(&field.field_type);

        if is_array {
            if let PathSource::Single(path) = &field.path_source {
                let inner_type = coerce::array_inner_type(&field.field_type);
                if let Some(value) = query::evaluate_wildcard_mapped(
                    env,
                    body,
                    path,
                    nil,
                    flat_keys,
                    &mut query_cache,
                    |elem| {
                        coerce::coerce_array_element(
                            env,
                            elem,
                            &field.field_type,
                            inner_type.as_ref(),
                            field.filter_nil,
                            nil,
                        )
                    },
                ) {
                    keys.push(crate::encode_string(env, &field.name));
                    values.push(value);
                    continue;
                }
            }
        }

        // For Enum8 fields, use a special resolution flow:
        // resolve raw value (no default), then enum8 handler does lookup + inference + default
        let is_enum8 = matches!(&field.field_type, FieldType::Enum8 { .. });

        let value = if is_enum8 {
            resolve_value_raw(env, body, field, &values, nil, flat_keys, &mut query_cache)
        } else {
            resolve_value(env, body, field, &values, nil, flat_keys, &mut query_cache)
        };

        // Array types skip transform, allowed_values, value_map, enum8, and json operations
        if is_array {
            let value = coerce::coerce_array(env, value, &field.field_type, field.filter_nil, nil);
            keys.push(crate::encode_string(env, &field.name));
            values.push(value);
            continue;
        }

        // Apply transform if configured
        let value = match &field.transform {
            Some(transform) => coerce::apply_transform(env, value, transform, nil),
            None => value,
        };

        // Check allowed_values whitelist
        let value = if !field.allowed_values.is_empty() && value != nil {
            if let Ok(b) = value.decode::<Binary>() {
                if field.allowed_values.contains(b.as_slice()) {
                    value
                } else {
                    coerce::encode_default(env, &field.default, nil)
                }
            } else {
                // Non-string values are not in the allowed list, fall back to default
                coerce::encode_default(env, &field.default, nil)
            }
        } else {
            value
        };

        // Apply value_map if configured. The variant was decided at compile
        // time (see `decode_field`): string fields populate `value_map_str` (a
        // string->string remap), all non-string fields populate `value_map` (a
        // string->integer lookup, e.g. severity_text -> severity_number). At most
        // one is non-empty, so this is a cheap branch, not per-event inference.
        // Either way, values absent from the map fall back to the default.
        let value = if !field.value_map_str.is_empty() {
            let mapped = coerce::apply_value_map_str(env, value, &field.value_map_str, nil);
            if mapped != nil {
                mapped
            } else {
                coerce::encode_default(env, &field.default, nil)
            }
        } else if !field.value_map.is_empty() {
            let mapped = coerce::apply_value_map(env, value, &field.value_map, nil);
            if mapped != nil {
                mapped
            } else {
                coerce::encode_default(env, &field.default, nil)
            }
        } else {
            value
        };

        // For Enum8 fields, handle enum resolution (string->int lookup + inference + default)
        let value = if is_enum8 {
            resolve_enum8(env, body, field, value, nil, flat_keys, &mut query_cache)
        } else {
            value
        };

        let value = match field.field_type {
            FieldType::Json => {
                if flat_keys {
                    apply_json_operations_flat(env, body, field, value, nil, &mut query_cache)
                } else {
                    apply_json_operations(env, body, field, value, nil, false, &mut query_cache)
                }
            }
            FieldType::FlatMap => {
                if flat_keys {
                    let value =
                        apply_json_operations_flat(env, body, field, value, nil, &mut query_cache);
                    stringify_values(env, value, nil)
                } else {
                    flatten_field(env, body, field, value, nil, &mut query_cache)
                }
            }
            _ => coerce::coerce(env, value, &field.field_type, nil),
        };

        keys.push(crate::encode_string(env, &field.name));
        values.push(value);
    }

    // Build the output map in a single allocation via enif_make_map_from_arrays.
    // Duplicate field names are rejected at compile time so this should not fail.
    Term::map_from_term_arrays(env, &keys, &values).unwrap_or_else(|_| Term::map_new(env))
}

/// Resolve the source value without applying defaults (for enum8 fields).
fn resolve_value_raw<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    output_values: &[Term<'a>],
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    match &field.path_source {
        PathSource::Root => body,
        PathSource::Single(path) => query::evaluate(env, body, path, nil, flat_keys, cache),
        PathSource::Coalesce(paths) => {
            query::evaluate_first(env, body, paths, (false, None), nil, flat_keys, cache)
        }
        PathSource::FromOutput(idx) => {
            let v = output_values[*idx];
            if v != nil {
                v
            } else {
                nil
            }
        }
        PathSource::FromOutputName(_) => unreachable!("FromOutputName should be resolved"),
    }
}

/// Resolve the source value for a field from the input document.
fn resolve_value<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    output_values: &[Term<'a>],
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    let skip_empty = field.field_type == FieldType::String;

    match &field.path_source {
        PathSource::Root => body,
        PathSource::Single(path) => {
            let v = query::evaluate(env, body, path, nil, flat_keys, cache);
            if v == nil {
                coerce::encode_default(env, &field.default, nil)
            } else if skip_empty {
                // Check binary length without allocating a String
                if let Ok(b) = v.decode::<Binary>() {
                    if b.is_empty() {
                        coerce::encode_default(env, &field.default, nil)
                    } else if let Some(ref f) = field.filters {
                        if !string_filters::passes_filters(b.as_slice(), f) {
                            coerce::encode_default(env, &field.default, nil)
                        } else {
                            v
                        }
                    } else {
                        v
                    }
                } else {
                    v
                }
            } else {
                v
            }
        }
        PathSource::Coalesce(paths) => {
            let string_filters = field.filters.as_ref();
            let result = query::evaluate_first(
                env,
                body,
                paths,
                (skip_empty, string_filters),
                nil,
                flat_keys,
                cache,
            );
            if result == nil {
                coerce::encode_default(env, &field.default, nil)
            } else {
                result
            }
        }
        PathSource::FromOutput(idx) => {
            let v = output_values[*idx];
            if v != nil {
                v
            } else {
                coerce::encode_default(env, &field.default, nil)
            }
        }
        PathSource::FromOutputName(_) => unreachable!("FromOutputName should be resolved"),
    }
}

/// Resolve Enum8 value: explicit path -> string lookup in enum_values, then inference.
fn resolve_enum8<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    resolved_value: Term<'a>,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    let enum8_data = match &field.enum8_data {
        Some(data) => data,
        None => return coerce::encode_default(env, &field.default, nil),
    };

    // Step 1: If resolved value is a string, look up in enum_values (case-insensitive)
    if resolved_value != nil {
        // Keys are pre-lowercased at compile time
        if let Some(val) = coerce::case_insensitive_get(&enum8_data.value_map, resolved_value) {
            return (*val as i64).encode(env);
        }
        // If it's already an integer, pass through
        if resolved_value.decode::<i64>().is_ok() {
            return resolved_value;
        }
    }

    // Step 2: Try inference rules
    if let Some(result_val) = evaluate_infer_rules(env, body, enum8_data, nil, flat_keys, cache) {
        return (result_val as i64).encode(env);
    }

    // Step 3: Default
    coerce::encode_default(env, &field.default, nil)
}

/// Apply JSON-specific operations: pick, exclude_keys, elevate_keys.
fn apply_json_operations<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    value: Term<'a>,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    let value = select_json_value(env, body, field, value, nil, flat_keys, cache);

    if value == nil || !value.is_map() {
        return value;
    }

    // Apply exclude_keys
    let value = if !field.exclude_keys.is_empty() {
        apply_exclude_keys(env, value, &field.exclude_keys)
    } else {
        value
    };

    // Apply elevate_keys
    let value = if !field.elevate_keys.is_empty() {
        apply_elevate_keys(env, value, &field.elevate_keys)
    } else {
        value
    };

    value
}

fn select_json_value<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    value: Term<'a>,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    if field.pick.is_empty() {
        return value;
    }

    let picked = build_pick_map(env, body, field, nil, flat_keys, cache);
    if picked == nil {
        value
    } else {
        picked
    }
}

/// Build a sparse map from pick entries.
fn build_pick_map<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    let mut keys: Vec<Term<'a>> = Vec::with_capacity(field.pick.len());
    let mut values: Vec<Term<'a>> = Vec::with_capacity(field.pick.len());

    for entry in &field.pick {
        let value = query::evaluate_first(
            env,
            body,
            &entry.paths,
            (false, None),
            nil,
            flat_keys,
            cache,
        );
        if value != nil {
            keys.push(crate::encode_string(env, &entry.key));
            values.push(value);
        }
    }

    if keys.is_empty() {
        nil
    } else {
        Term::map_from_term_arrays(env, &keys, &values).unwrap_or_else(|_| Term::map_new(env))
    }
}

/// Remove specified keys from a map.
fn apply_exclude_keys<'a>(env: Env<'a>, map: Term<'a>, exclude: &[Vec<u8>]) -> Term<'a> {
    let iter = match MapIterator::new(map) {
        Some(it) => it,
        None => return map,
    };

    let mut keys: Vec<Term<'a>> = Vec::with_capacity(32);
    let mut values: Vec<Term<'a>> = Vec::with_capacity(32);

    for (k, v) in iter {
        if let Ok(b) = k.decode::<Binary>() {
            if exclude.iter().any(|ek| ek.as_slice() == b.as_slice()) {
                continue;
            }
        }
        keys.push(k);
        values.push(v);
    }

    Term::map_from_term_arrays(env, &keys, &values).unwrap_or_else(|_| Term::map_new(env))
}

/// Elevate children of specified keys into the parent map.
/// Existing top-level keys win over elevated children.
///
/// Uses map_from_term_arrays for the elevated children base, then map_put
/// for top-level overrides. Cannot use a single map_from_term_arrays call
/// because duplicate keys (between elevated children and top-level) would
/// cause enif_make_map_from_arrays to fail.
fn apply_elevate_keys<'a>(env: Env<'a>, map: Term<'a>, elevate: &[Vec<u8>]) -> Term<'a> {
    if elevate.len() > 1 {
        return apply_multiple_elevate_keys(env, map, elevate);
    }

    let iter = match MapIterator::new(map) {
        Some(it) => it,
        None => return map,
    };

    // Collect top-level entries, separating elevated vs non-elevated
    let mut top_entries: Vec<(Term<'a>, Term<'a>)> = Vec::with_capacity(32);
    let mut elevated_keys: Vec<Term<'a>> = Vec::with_capacity(16);
    let mut elevated_values: Vec<Term<'a>> = Vec::with_capacity(16);

    for (k, v) in iter {
        if let Ok(b) = k.decode::<Binary>() {
            if elevate.iter().any(|ek| ek.as_slice() == b.as_slice()) {
                // This key should be elevated: merge its children into parent
                if let Some(child_iter) = MapIterator::new(v) {
                    for (ck, cv) in child_iter {
                        elevated_keys.push(ck);
                        elevated_values.push(cv);
                    }
                }
                continue; // Don't include the elevated key itself
            }
        }
        top_entries.push((k, v));
    }

    // Build base map from elevated children in a single allocation
    let mut result = if elevated_keys.is_empty() {
        Term::map_new(env)
    } else {
        Term::map_from_term_arrays(env, &elevated_keys, &elevated_values)
            .unwrap_or_else(|_| Term::map_new(env))
    };

    // Top-level keys overwrite elevated children via map_put
    for (k, v) in top_entries {
        result = result.map_put(k, v).unwrap_or(result);
    }

    result
}

fn apply_multiple_elevate_keys<'a>(env: Env<'a>, map: Term<'a>, elevate: &[Vec<u8>]) -> Term<'a> {
    let mut result = Term::map_new(env);

    // Apply in reverse so the first configured elevate key wins when child
    // maps contain the same key. Top-level entries are applied afterwards and
    // retain the documented highest precedence.
    for elevate_key in elevate.iter().rev() {
        let Ok(elevate_key) = std::str::from_utf8(elevate_key) else {
            continue;
        };
        let Ok(child) = map.map_get(crate::encode_string(env, elevate_key)) else {
            continue;
        };
        let Some(children) = MapIterator::new(child) else {
            continue;
        };

        for (key, value) in children {
            result = result.map_put(key, value).unwrap_or(result);
        }
    }

    if let Some(entries) = MapIterator::new(map) {
        for (key, value) in entries {
            let elevated = key
                .decode::<Binary>()
                .ok()
                .is_some_and(|binary| elevate.iter().any(|item| item == binary.as_slice()));
            if !elevated {
                result = result.map_put(key, value).unwrap_or(result);
            }
        }
    }

    result
}

/// Apply JSON operations for flat-key input.
///
/// Flat-key variants of exclude/elevate operate on dot-notation prefixes
/// rather than exact key matches on nested maps.
fn apply_json_operations_flat<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    value: Term<'a>,
    nil: Term<'a>,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    let value = select_json_value(env, body, field, value, nil, true, cache);

    if value == nil || !value.is_map() {
        return value;
    }

    let value = if !field.exclude_keys.is_empty() {
        apply_exclude_keys_flat(env, value, &field.exclude_keys)
    } else {
        value
    };

    let value = if !field.elevate_keys.is_empty() {
        apply_elevate_keys_flat(env, value, &field.elevate_keys)
    } else {
        value
    };

    value
}

/// Remove keys that match exactly OR start with `"<exclude_key>."`.
fn apply_exclude_keys_flat<'a>(env: Env<'a>, map: Term<'a>, exclude: &[Vec<u8>]) -> Term<'a> {
    let iter = match MapIterator::new(map) {
        Some(it) => it,
        None => return map,
    };

    let mut keys: Vec<Term<'a>> = Vec::with_capacity(32);
    let mut values: Vec<Term<'a>> = Vec::with_capacity(32);

    for (k, v) in iter {
        if let Ok(b) = k.decode::<Binary>() {
            let key_bytes = b.as_slice();
            let excluded = exclude.iter().any(|ek| {
                key_bytes == ek.as_slice()
                    || (key_bytes.len() > ek.len()
                        && key_bytes.starts_with(ek.as_slice())
                        && key_bytes[ek.len()] == b'.')
            });
            if excluded {
                continue;
            }
        }
        keys.push(k);
        values.push(v);
    }

    Term::map_from_term_arrays(env, &keys, &values).unwrap_or_else(|_| Term::map_new(env))
}

/// Elevate flat keys: keys starting with `"<elevate_key>."` get their prefix
/// stripped. The elevate key itself (exact match) is dropped. Top-level keys
/// (those not starting with any elevate prefix) win over elevated children.
fn apply_elevate_keys_flat<'a>(env: Env<'a>, map: Term<'a>, elevate: &[Vec<u8>]) -> Term<'a> {
    if elevate.len() > 1 {
        return apply_multiple_elevate_keys_flat(env, map, elevate);
    }

    let iter = match MapIterator::new(map) {
        Some(it) => it,
        None => return map,
    };

    let mut top_entries: Vec<(Term<'a>, Term<'a>)> = Vec::with_capacity(32);
    let mut elevated_keys: Vec<Term<'a>> = Vec::with_capacity(16);
    let mut elevated_values: Vec<Term<'a>> = Vec::with_capacity(16);

    for (k, v) in iter {
        if let Ok(b) = k.decode::<Binary>() {
            let key_bytes = b.as_slice();

            // Check if this key matches an elevate prefix
            let mut matched = false;
            for ek in elevate {
                if key_bytes == ek.as_slice() {
                    // Exact match — drop the key entirely
                    matched = true;
                    break;
                }
                if key_bytes.len() > ek.len()
                    && key_bytes.starts_with(ek.as_slice())
                    && key_bytes[ek.len()] == b'.'
                {
                    // Strip prefix: "metadata.level" -> "level"
                    let suffix = &key_bytes[ek.len() + 1..];
                    let suffix_term =
                        crate::encode_string(env, std::str::from_utf8(suffix).unwrap_or(""));
                    elevated_keys.push(suffix_term);
                    elevated_values.push(v);
                    matched = true;
                    break;
                }
            }
            if !matched {
                top_entries.push((k, v));
            }
        } else {
            top_entries.push((k, v));
        }
    }

    // Build base map from elevated children
    let mut result = if elevated_keys.is_empty() {
        Term::map_new(env)
    } else {
        Term::map_from_term_arrays(env, &elevated_keys, &elevated_values)
            .unwrap_or_else(|_| Term::map_new(env))
    };

    // Top-level keys overwrite elevated children
    for (k, v) in top_entries {
        result = result.map_put(k, v).unwrap_or(result);
    }

    result
}

fn apply_multiple_elevate_keys_flat<'a>(
    env: Env<'a>,
    map: Term<'a>,
    elevate: &[Vec<u8>],
) -> Term<'a> {
    let Some(entries) = MapIterator::new(map) else {
        return map;
    };

    let mut elevated_entries = vec![Vec::new(); elevate.len()];
    let mut top_entries = Vec::new();

    for (key, value) in entries {
        let Ok(binary) = key.decode::<Binary>() else {
            top_entries.push((key, value));
            continue;
        };
        let key_bytes = binary.as_slice();
        let mut matched = false;

        for (index, elevate_key) in elevate.iter().enumerate() {
            if key_bytes == elevate_key.as_slice() {
                matched = true;
                break;
            }
            if key_bytes.len() > elevate_key.len()
                && key_bytes.starts_with(elevate_key.as_slice())
                && key_bytes[elevate_key.len()] == b'.'
            {
                let suffix = &key_bytes[elevate_key.len() + 1..];
                let suffix = crate::encode_string(env, std::str::from_utf8(suffix).unwrap_or(""));
                elevated_entries[index].push((suffix, value));
                matched = true;
                break;
            }
        }

        if !matched {
            top_entries.push((key, value));
        }
    }

    let mut result = Term::map_new(env);
    for entries in elevated_entries.into_iter().rev() {
        for (key, value) in entries {
            result = result.map_put(key, value).unwrap_or(result);
        }
    }
    for (key, value) in top_entries {
        result = result.map_put(key, value).unwrap_or(result);
    }

    result
}

/// Evaluate inference conditions and return the matching rule's result.
pub fn evaluate_infer_rules<'a>(
    env: Env<'a>,
    body: Term<'a>,
    enum8_data: &Enum8Data,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> Option<i8> {
    for rule in &enum8_data.infer_rules {
        let any_match = if rule.any.is_empty() {
            true
        } else {
            rule.any
                .iter()
                .any(|cond| evaluate_condition(env, body, cond, nil, flat_keys, cache))
        };

        let all_match = if rule.all.is_empty() {
            true
        } else {
            rule.all
                .iter()
                .all(|cond| evaluate_condition(env, body, cond, nil, flat_keys, cache))
        };

        if any_match && all_match {
            // Both result and value_map keys are pre-lowercased at compile time
            if let Some(val) = enum8_data.value_map.get(&rule.result) {
                return Some(*val);
            }
        }
    }

    None
}

/// Evaluate a single inference condition.
fn evaluate_condition<'a>(
    env: Env<'a>,
    body: Term<'a>,
    cond: &crate::mapping::InferCondition,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut query::QueryCache<'a>,
) -> bool {
    let value = query::evaluate(env, body, &cond.path, nil, flat_keys, cache);

    match &cond.predicate {
        Predicate::Exists => value != nil,
        Predicate::NotExists => value == nil,
        Predicate::NotZero => {
            if value == nil {
                return false;
            }
            if let Ok(i) = value.decode::<i64>() {
                i != 0
            } else if let Ok(f) = value.decode::<f64>() {
                f != 0.0
            } else {
                false
            }
        }
        Predicate::IsZero => {
            if value == nil {
                return false;
            }
            if let Ok(i) = value.decode::<i64>() {
                i == 0
            } else if let Ok(f) = value.decode::<f64>() {
                f == 0.0
            } else {
                false
            }
        }
        Predicate::GreaterThan(threshold) => {
            if value == nil {
                return false;
            }
            if let Ok(i) = value.decode::<i64>() {
                (i as f64) > *threshold
            } else if let Ok(f) = value.decode::<f64>() {
                f > *threshold
            } else {
                false
            }
        }
        Predicate::LessThan(threshold) => {
            if value == nil {
                return false;
            }
            if let Ok(i) = value.decode::<i64>() {
                (i as f64) < *threshold
            } else if let Ok(f) = value.decode::<f64>() {
                f < *threshold
            } else {
                false
            }
        }
        Predicate::NotEmpty => {
            if value == nil {
                return false;
            }
            if let Ok(b) = value.decode::<Binary>() {
                !b.is_empty()
            } else if let Ok(mut iter) = value.decode::<ListIterator>() {
                iter.next().is_some()
            } else {
                false
            }
        }
        Predicate::IsEmpty => {
            if value == nil {
                return false;
            }
            if let Ok(b) = value.decode::<Binary>() {
                b.is_empty()
            } else if let Ok(mut iter) = value.decode::<ListIterator>() {
                iter.next().is_none()
            } else {
                false
            }
        }
        Predicate::Equals(expected) => {
            if value == nil {
                return false;
            }
            matches_predicate_value(value, expected)
        }
        Predicate::NotEquals(expected) => {
            if value == nil {
                return true;
            }
            !matches_predicate_value(value, expected)
        }
        Predicate::In(values) => {
            if value == nil {
                return false;
            }
            values.iter().any(|v| matches_predicate_value(value, v))
        }
        Predicate::IsString => value != nil && value.is_binary(),
        Predicate::IsNumber => {
            value != nil && (value.decode::<i64>().is_ok() || value.decode::<f64>().is_ok())
        }
        Predicate::IsList => value != nil && value.is_list(),
        Predicate::IsMap => value != nil && value.is_map(),
    }
}

/// Check if a BEAM term matches a predicate value.
fn matches_predicate_value(term: Term, expected: &PredicateValue) -> bool {
    match expected {
        PredicateValue::Str(s) => {
            if let Ok(b) = term.decode::<Binary>() {
                b.as_slice() == s.as_bytes()
            } else {
                false
            }
        }
        PredicateValue::Int(i) => {
            if let Ok(ti) = term.decode::<i64>() {
                ti == *i
            } else {
                false
            }
        }
        PredicateValue::Flt(f) => {
            if let Ok(tf) = term.decode::<f64>() {
                (tf - f).abs() < f64::EPSILON
            } else {
                false
            }
        }
        PredicateValue::Bool(b) => {
            if let Ok(tb) = term.decode::<bool>() {
                tb == *b
            } else {
                false
            }
        }
    }
}

// ── FlatMap: flatten nested maps to dot-notation keys with string values ─────

fn flatten_field<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    value: Term<'a>,
    nil: Term<'a>,
    cache: &mut query::QueryCache<'a>,
) -> Term<'a> {
    let value = select_json_value(env, body, field, value, nil, false, cache);

    if field.exclude_keys.is_empty() && field.elevate_keys.is_empty() {
        return flatten_and_stringify(env, value, nil);
    }

    if let Some(flattened) =
        try_flatten_with_operations(env, value, &field.exclude_keys, &field.elevate_keys, nil)
    {
        return flattened;
    }

    let value = if field.exclude_keys.is_empty() {
        value
    } else {
        apply_exclude_keys(env, value, &field.exclude_keys)
    };
    let value = if field.elevate_keys.is_empty() {
        value
    } else {
        apply_elevate_keys(env, value, &field.elevate_keys)
    };
    flatten_and_stringify(env, value, nil)
}

fn try_flatten_with_operations<'a>(
    env: Env<'a>,
    value: Term<'a>,
    exclude: &[Vec<u8>],
    elevate: &[Vec<u8>],
    nil: Term<'a>,
) -> Option<Term<'a>> {
    if value == nil || !value.is_map() {
        return Some(Term::map_new(env));
    }
    if elevate.len() > 1 {
        return None;
    }

    let capacity = value.map_size().unwrap_or(0);
    let mut keys = Vec::with_capacity(capacity);
    let mut values = Vec::with_capacity(capacity);
    let mut prefix = String::new();

    for (key, child) in MapIterator::new(value)? {
        let binary = match key.decode::<Binary>() {
            Ok(binary) => binary,
            Err(_) => continue,
        };
        let bytes = binary.as_slice();

        if exclude.iter().any(|excluded| excluded.as_slice() == bytes) {
            continue;
        }
        if elevate.iter().any(|elevated| elevated.as_slice() == bytes) {
            if let Some(children) = MapIterator::new(child) {
                for (child_key, child_value) in children {
                    if top_level_wins(value, child_key, exclude, elevate) {
                        continue;
                    }
                    flatten_map_entry(
                        env,
                        child_key,
                        child_value,
                        &mut prefix,
                        &mut keys,
                        &mut values,
                        nil,
                    );
                }
            }
            continue;
        }

        flatten_map_entry(env, key, child, &mut prefix, &mut keys, &mut values, nil);
    }

    if keys.is_empty() {
        Some(Term::map_new(env))
    } else {
        Term::map_from_term_arrays(env, &keys, &values).ok()
    }
}

fn top_level_wins(map: Term<'_>, key: Term<'_>, exclude: &[Vec<u8>], elevate: &[Vec<u8>]) -> bool {
    if map.map_get(key).is_err() {
        return false;
    }

    let binary = match key.decode::<Binary>() {
        Ok(binary) => binary,
        Err(_) => return false,
    };
    let bytes = binary.as_slice();

    !exclude
        .iter()
        .any(|candidate| candidate.as_slice() == bytes)
        && !elevate
            .iter()
            .any(|candidate| candidate.as_slice() == bytes)
}

/// Flatten a potentially nested map into `%{String.t() => String.t()}`.
///
/// - Nested maps: `%{"a" => %{"b" => 1}}` → `%{"a.b" => "1"}`
/// - Lists: `%{"a" => [1, 2]}` → `%{"a" => "[1,2]"}`
/// - Scalars: coerced to string (`integer.to_string()`, `"true"`, etc.)
/// - nil values: omitted from output
/// - Empty/nil input: returns `%{}`
pub fn flatten_and_stringify<'a>(env: Env<'a>, value: Term<'a>, nil: Term<'a>) -> Term<'a> {
    if value == nil || !value.is_map() {
        return Term::map_new(env);
    }

    let capacity = value.map_size().unwrap_or(0);
    let mut keys: Vec<Term<'a>> = Vec::with_capacity(capacity);
    let mut values: Vec<Term<'a>> = Vec::with_capacity(capacity);
    let mut prefix = String::new();
    flatten_map_recursive(env, value, &mut prefix, &mut keys, &mut values, nil);

    build_flat_map(env, &keys, &values)
}

/// Stringify values in an already-flat map without recursive flattening.
///
/// Used when `flat_keys` is true — the input is already single-level with
/// dot-notation keys, so we only need to coerce values to strings.
/// nil values are omitted. Lists are JSON-encoded.
pub fn stringify_values<'a>(env: Env<'a>, value: Term<'a>, nil: Term<'a>) -> Term<'a> {
    if value == nil || !value.is_map() {
        return Term::map_new(env);
    }

    let iter = match MapIterator::new(value) {
        Some(it) => it,
        None => return Term::map_new(env),
    };

    let capacity = value.map_size().unwrap_or(0);
    let mut keys: Vec<Term<'a>> = Vec::with_capacity(capacity);
    let mut values: Vec<Term<'a>> = Vec::with_capacity(capacity);

    for (k, v) in iter {
        if v == nil {
            continue;
        }

        // If value is already a string, pass through without allocation
        if v.is_binary() {
            keys.push(k);
            values.push(v);
            continue;
        }

        keys.push(k);
        values.push(term_to_string_term(env, v, nil));
    }

    if keys.is_empty() {
        Term::map_new(env)
    } else {
        Term::map_from_term_arrays(env, &keys, &values).unwrap_or_else(|_| Term::map_new(env))
    }
}

fn flatten_map_recursive<'a>(
    env: Env<'a>,
    map: Term<'a>,
    prefix: &mut String,
    keys: &mut Vec<Term<'a>>,
    values: &mut Vec<Term<'a>>,
    nil: Term<'a>,
) {
    let iter = match MapIterator::new(map) {
        Some(it) => it,
        None => return,
    };

    for (key, value) in iter {
        flatten_map_entry(env, key, value, prefix, keys, values, nil);
    }
}

fn flatten_map_entry<'a>(
    env: Env<'a>,
    key: Term<'a>,
    value: Term<'a>,
    prefix: &mut String,
    keys: &mut Vec<Term<'a>>,
    values: &mut Vec<Term<'a>>,
    nil: Term<'a>,
) {
    let binary = match key.decode::<Binary>() {
        Ok(binary) => binary,
        Err(_) => return,
    };
    let key = match std::str::from_utf8(binary.as_slice()) {
        Ok(key) => key,
        Err(_) => return,
    };

    let prefix_len = prefix.len();
    if prefix_len != 0 {
        prefix.push('.');
    }
    prefix.push_str(key);

    if value == nil {
        prefix.truncate(prefix_len);
        return;
    }

    if value.is_map() {
        if value.map_size().unwrap_or(0) == 0 {
            keys.push(crate::encode_string(env, prefix));
            values.push(crate::encode_string(env, "{}"));
        } else {
            flatten_map_recursive(env, value, prefix, keys, values, nil);
        }
        prefix.truncate(prefix_len);
        return;
    }

    keys.push(crate::encode_string(env, prefix));
    if value.is_list() {
        values.push(crate::encode_string(
            env,
            &term_to_json_string(value, nil, "[]"),
        ));
    } else {
        values.push(term_to_string_term(env, value, nil));
    }
    prefix.truncate(prefix_len);
}

fn build_flat_map<'a>(env: Env<'a>, keys: &[Term<'a>], values: &[Term<'a>]) -> Term<'a> {
    if keys.is_empty() {
        return Term::map_new(env);
    }

    Term::map_from_term_arrays(env, keys, values).unwrap_or_else(|_| {
        let mut result = Term::map_new(env);
        for (key, value) in keys.iter().copied().zip(values.iter().copied()) {
            result = result.map_put(key, value).unwrap_or(result);
        }
        result
    })
}

fn term_to_string_term<'a>(env: Env<'a>, value: Term<'a>, nil: Term<'a>) -> Term<'a> {
    if let Ok(binary) = value.decode::<Binary>() {
        return if std::str::from_utf8(binary.as_slice()).is_ok() {
            value
        } else {
            crate::encode_string(env, "")
        };
    }

    if let Ok(i) = value.decode::<i64>() {
        return crate::encode_integer(env, i);
    }
    if let Ok(f) = value.decode::<f64>() {
        return crate::encode_string(env, &f.to_string());
    }
    if let Ok(b) = value.decode::<bool>() {
        return crate::encode_string(env, if b { "true" } else { "false" });
    }
    if value.is_atom() {
        return value
            .atom_to_string()
            .map(|atom| crate::encode_string(env, &atom))
            .unwrap_or_else(|_| crate::encode_string(env, ""));
    }
    if value.is_map() || value.is_list() {
        let fallback = if value.is_map() { "{}" } else { "[]" };
        return crate::encode_string(env, &term_to_json_string(value, nil, fallback));
    }

    crate::encode_string(env, "")
}

fn term_to_json_string<'a>(value: Term<'a>, nil: Term<'a>, fallback: &str) -> String {
    serde_json::to_string(&JsonTerm { value, nil }).unwrap_or_else(|_| fallback.to_string())
}

struct JsonTerm<'a> {
    value: Term<'a>,
    nil: Term<'a>,
}

impl Serialize for JsonTerm<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if self.value == self.nil {
            return serializer.serialize_none();
        }
        if let Ok(value) = self.value.decode::<bool>() {
            return serializer.serialize_bool(value);
        }
        if let Ok(value) = self.value.decode::<i64>() {
            return serializer.serialize_i64(value);
        }
        if let Ok(value) = self.value.decode::<f64>() {
            return if value.is_finite() {
                serializer.serialize_f64(value)
            } else {
                serializer.serialize_none()
            };
        }
        if let Ok(binary) = self.value.decode::<Binary>() {
            return match std::str::from_utf8(binary.as_slice()) {
                Ok(value) => serializer.serialize_str(value),
                Err(_) => serializer.serialize_none(),
            };
        }
        if self.value.is_atom() {
            return match self.value.atom_to_string() {
                Ok(value) => serializer.serialize_str(&value),
                Err(_) => serializer.serialize_none(),
            };
        }
        if let Ok(iter) = self.value.decode::<ListIterator>() {
            let mut sequence = serializer.serialize_seq(None)?;
            for value in iter {
                sequence.serialize_element(&JsonTerm {
                    value,
                    nil: self.nil,
                })?;
            }
            return sequence.end();
        }
        if let Some(iter) = MapIterator::new(self.value) {
            let mut map = serializer.serialize_map(None)?;
            for (key, value) in iter {
                let Ok(binary) = key.decode::<Binary>() else {
                    continue;
                };
                let Ok(key) = std::str::from_utf8(binary.as_slice()) else {
                    continue;
                };
                map.serialize_entry(
                    key,
                    &JsonTerm {
                        value,
                        nil: self.nil,
                    },
                )?;
            }
            return map.end();
        }

        serializer.serialize_none()
    }
}
