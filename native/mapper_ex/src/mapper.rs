use rustler::types::list::ListIterator;
use rustler::types::map::MapIterator;
use rustler::{Binary, Encoder, Env, Term};

use crate::coerce;
use crate::mapping::{
    CompiledField, CompiledMapping, Enum8Data, FieldType, PathSource, Predicate, PredicateValue,
};
use crate::query;

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
    )
}

/// Execute the mapping on a single document, returning the mapped output map.
pub fn map_single<'a>(env: Env<'a>, body: Term<'a>, mapping: &CompiledMapping) -> Term<'a> {
    let nil = atoms::nil().encode(env);
    let field_count = mapping.fields.len();

    let mut keys: Vec<Term<'a>> = Vec::with_capacity(field_count);
    let mut values: Vec<Term<'a>> = Vec::with_capacity(field_count);

    for field in &mapping.fields {
        let is_array = is_array_type(&field.field_type);

        // For Enum8 fields, use a special resolution flow:
        // resolve raw value (no default), then enum8 handler does lookup + inference + default
        let is_enum8 = matches!(&field.field_type, FieldType::Enum8 { .. });

        let value = if is_enum8 {
            resolve_value_raw(env, body, field, &values, nil)
        } else {
            resolve_value(env, body, field, &values, nil)
        };

        // Array types skip transform, allowed_values, value_map, enum8, and json operations
        if is_array {
            let value = coerce::coerce_array(env, value, &field.field_type, field.filter_nil, nil);
            keys.push(field.name.encode(env));
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
                value // non-string values bypass the check
            }
        } else {
            value
        };

        // Apply value_map if configured (e.g., severity_text -> severity_number)
        let value = if !field.value_map.is_empty() {
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
            resolve_enum8(env, body, field, value, nil)
        } else {
            value
        };

        // For Json fields, apply exclude/elevate/pick
        let value = if field.field_type == FieldType::Json {
            apply_json_operations(env, body, field, value, nil)
        } else {
            value
        };

        // Apply type coercion (skip for Json pass-through)
        let value = if field.field_type == FieldType::Json {
            value
        } else {
            coerce::coerce(env, value, &field.field_type, nil)
        };

        keys.push(field.name.encode(env));
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
) -> Term<'a> {
    match &field.path_source {
        PathSource::Root => body,
        PathSource::Single(segments) => query::evaluate(env, body, segments, nil),
        PathSource::Coalesce(paths) => query::evaluate_first(env, body, paths, false, nil),
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
) -> Term<'a> {
    let skip_empty = field.field_type == FieldType::String;

    match &field.path_source {
        PathSource::Root => body,
        PathSource::Single(segments) => {
            let v = query::evaluate(env, body, segments, nil);
            if v == nil {
                coerce::encode_default(env, &field.default, nil)
            } else if skip_empty {
                // Check binary length without allocating a String
                if let Ok(b) = v.decode::<Binary>() {
                    if b.is_empty() {
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
        }
        PathSource::Coalesce(paths) => {
            let result = query::evaluate_first(env, body, paths, skip_empty, nil);
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
    if let Some(result_val) = evaluate_infer_rules(env, body, enum8_data, nil) {
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
) -> Term<'a> {
    // Pick-first: if pick is defined, try to build a sparse map
    let value = if !field.pick.is_empty() {
        let pick_result = build_pick_map(env, body, field, nil);
        if pick_result != nil {
            pick_result
        } else {
            // Pick produced empty map, fall back to path-resolved value
            value
        }
    } else {
        value
    };

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

/// Build a sparse map from pick entries.
fn build_pick_map<'a>(
    env: Env<'a>,
    body: Term<'a>,
    field: &CompiledField,
    nil: Term<'a>,
) -> Term<'a> {
    let mut keys: Vec<Term<'a>> = Vec::with_capacity(field.pick.len());
    let mut values: Vec<Term<'a>> = Vec::with_capacity(field.pick.len());

    for entry in &field.pick {
        let value = query::evaluate_first(env, body, &entry.paths, false, nil);
        if value != nil {
            keys.push(entry.key.encode(env));
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

/// Evaluate inference conditions and return the matching rule's result.
pub fn evaluate_infer_rules<'a>(
    env: Env<'a>,
    body: Term<'a>,
    enum8_data: &Enum8Data,
    nil: Term<'a>,
) -> Option<i8> {
    for rule in &enum8_data.infer_rules {
        let any_match = if rule.any.is_empty() {
            true
        } else {
            rule.any
                .iter()
                .any(|cond| evaluate_condition(env, body, cond, nil))
        };

        let all_match = if rule.all.is_empty() {
            true
        } else {
            rule.all
                .iter()
                .all(|cond| evaluate_condition(env, body, cond, nil))
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
) -> bool {
    let value = query::evaluate(env, body, &cond.path, nil);

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
