use std::collections::HashMap;
use std::collections::HashSet;

use rustler::types::map::MapIterator;
use rustler::{Encoder, Env, Term};

use crate::path::{self, PathSegment};

// ── Data structures ────────────────────────────────────────────────────────

#[derive(Debug)]
pub struct CompiledMapping {
    pub fields: Vec<CompiledField>,
}

#[derive(Debug)]
pub struct CompiledField {
    pub name: String,
    pub path_source: PathSource,
    pub field_type: FieldType,
    pub default: DefaultValue,
    pub transform: Option<FieldTransform>,
    pub allowed_values: HashSet<Vec<u8>>,
    pub value_map: HashMap<String, i64>,
    pub exclude_keys: Vec<Vec<u8>>,
    pub elevate_keys: Vec<Vec<u8>>,
    pub pick: Vec<PickEntry>,
    pub enum8_data: Option<Enum8Data>,
    pub filter_nil: bool,
    pub flat_map_value_type: FlatMapValueType,
}

#[derive(Debug)]
pub enum PathSource {
    Root,
    Single(Vec<PathSegment>),
    Coalesce(Vec<Vec<PathSegment>>),
    /// Index into the output values vector (resolved at compile time from field name).
    FromOutput(usize),
    /// Temporary: unresolved field name, converted to FromOutput(usize) during compilation.
    FromOutputName(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldType {
    String,
    UInt8,
    UInt32,
    UInt64,
    Int32,
    Float64,
    Bool,
    Enum8 {
        precision: u8, // unused, placeholder to keep enum variant distinct
    },
    DateTime64 {
        precision: u8,
    },
    Json,
    ArrayString,
    ArrayUInt64,
    ArrayFloat64,
    ArrayDateTime64 {
        precision: u8,
    },
    ArrayJson,
    ArrayMap,
    FlatMap,
    ArrayFlatMap,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FlatMapValueType {
    String,
}

#[derive(Debug, Clone)]
pub enum DefaultValue {
    Nil,
    Str(String),
    Int(i64),
    Uint(u64),
    Flt(f64),
    Bool(bool),
    EmptyList,
    EmptyMap,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum FieldTransform {
    Upcase,
    Downcase,
}

#[derive(Debug)]
pub struct PickEntry {
    pub key: String,
    pub paths: Vec<Vec<PathSegment>>,
}

/// Inference rule for Enum8 structural inference.
#[derive(Debug)]
pub struct InferRule {
    pub any: Vec<InferCondition>,
    pub all: Vec<InferCondition>,
    pub result: String,
}

#[derive(Debug)]
pub struct InferCondition {
    pub path: Vec<PathSegment>,
    pub predicate: Predicate,
}

#[derive(Debug)]
pub enum Predicate {
    Exists,
    NotExists,
    NotZero,
    IsZero,
    GreaterThan(f64),
    LessThan(f64),
    NotEmpty,
    IsEmpty,
    Equals(PredicateValue),
    NotEquals(PredicateValue),
    In(Vec<PredicateValue>),
    IsString,
    IsNumber,
    IsList,
    IsMap,
}

#[derive(Debug, Clone)]
pub enum PredicateValue {
    Str(String),
    Int(i64),
    Flt(f64),
    Bool(bool),
}

/// Enum8-specific compiled data stored alongside CompiledField.
#[derive(Debug)]
pub struct Enum8Data {
    pub value_map: HashMap<String, i8>,
    pub infer_rules: Vec<InferRule>,
}

// ── Config decoder ─────────────────────────────────────────────────────────

pub fn decode_mapping<'a>(env: Env<'a>, config: Term<'a>) -> Result<CompiledMapping, String> {
    let fields = decode_fields(env, config)?;
    Ok(CompiledMapping { fields })
}

fn decode_fields<'a>(env: Env<'a>, config: Term<'a>) -> Result<Vec<CompiledField>, String> {
    let fields_term = get_term_key(env, config, "fields")
        .ok_or_else(|| "missing 'fields' key in config".to_string())?;

    let field_list: Vec<Term> = fields_term
        .decode()
        .map_err(|_| "fields must be a list".to_string())?;

    let mut compiled_fields = Vec::with_capacity(field_list.len());
    let mut name_to_index: HashMap<String, usize> = HashMap::with_capacity(field_list.len());

    for field_term in field_list {
        let mut field = decode_field(env, field_term)?;

        if name_to_index.contains_key(&field.name) {
            return Err(format!("duplicate field name: '{}'", field.name));
        }

        // Resolve from_output field name to index
        if let PathSource::FromOutputName(ref name) = field.path_source {
            let idx = name_to_index.get(name).ok_or_else(|| {
                format!("from_output '{}' references unknown or later field", name)
            })?;
            field.path_source = PathSource::FromOutput(*idx);
        }

        let idx = compiled_fields.len();
        name_to_index.insert(field.name.clone(), idx);
        compiled_fields.push(field);
    }
    Ok(compiled_fields)
}

fn decode_field<'a>(env: Env<'a>, field: Term<'a>) -> Result<CompiledField, String> {
    let name =
        get_string_key(env, field, "name")?.ok_or_else(|| "field missing 'name'".to_string())?;

    let type_str = get_string_key(env, field, "type")?.unwrap_or_else(|| "string".to_string());
    let type_lower = type_str.to_lowercase();

    let field_type = parse_field_type(env, field, &type_lower)?;
    let default = decode_default(env, field, &field_type)?;
    let path_source = decode_path_source(env, field)?;
    let transform = decode_transform(env, field)?;
    let allowed_values = decode_allowed_values(env, field);
    let value_map = decode_value_map(env, field)?;
    let exclude_keys = decode_string_list_bytes(env, field, "exclude_keys");
    let elevate_keys = decode_string_list_bytes(env, field, "elevate_keys");
    let pick = decode_pick(env, field)?;

    let enum8_data = if matches!(field_type, FieldType::Enum8 { .. }) {
        Some(decode_enum8_data(env, field)?)
    } else {
        None
    };

    let filter_nil = decode_filter_nil(env, field);
    let flat_map_value_type = decode_flat_map_value_type(env, field)?;

    Ok(CompiledField {
        name,
        path_source,
        field_type,
        default,
        transform,
        allowed_values,
        value_map,
        exclude_keys,
        elevate_keys,
        pick,
        enum8_data,
        filter_nil,
        flat_map_value_type,
    })
}

fn parse_field_type<'a>(env: Env<'a>, field: Term<'a>, s: &str) -> Result<FieldType, String> {
    match s {
        "string" => Ok(FieldType::String),
        "uint8" => Ok(FieldType::UInt8),
        "uint32" => Ok(FieldType::UInt32),
        "uint64" => Ok(FieldType::UInt64),
        "int32" => Ok(FieldType::Int32),
        "float64" => Ok(FieldType::Float64),
        "bool" | "boolean" => Ok(FieldType::Bool),
        "enum8" => Ok(FieldType::Enum8 { precision: 0 }),
        "datetime64" => {
            let precision = get_int_key(env, field, "precision").unwrap_or(9) as u8;
            Ok(FieldType::DateTime64 { precision })
        }
        "json" => Ok(FieldType::Json),
        "array_string" => Ok(FieldType::ArrayString),
        "array_uint64" => Ok(FieldType::ArrayUInt64),
        "array_float64" => Ok(FieldType::ArrayFloat64),
        "array_datetime64" => {
            let precision = get_int_key(env, field, "precision").unwrap_or(9) as u8;
            Ok(FieldType::ArrayDateTime64 { precision })
        }
        "array_json" => Ok(FieldType::ArrayJson),
        "array_map" => Ok(FieldType::ArrayMap),
        "flat_map" => Ok(FieldType::FlatMap),
        "array_flat_map" => Ok(FieldType::ArrayFlatMap),
        other => Err(format!("unknown field type: {}", other)),
    }
}

fn decode_default<'a>(
    env: Env<'a>,
    field: Term<'a>,
    field_type: &FieldType,
) -> Result<DefaultValue, String> {
    let val = match get_term_key(env, field, "default") {
        Some(t) => t,
        None => {
            return Ok(match field_type {
                FieldType::Json | FieldType::FlatMap => DefaultValue::EmptyMap,
                FieldType::ArrayString
                | FieldType::ArrayUInt64
                | FieldType::ArrayFloat64
                | FieldType::ArrayDateTime64 { .. }
                | FieldType::ArrayJson
                | FieldType::ArrayMap
                | FieldType::ArrayFlatMap => DefaultValue::EmptyList,
                _ => DefaultValue::Nil,
            });
        }
    };

    // Check for nil atom
    if val.is_atom() {
        if let Ok(a) = rustler::types::atom::Atom::from_term(val) {
            if a == rustler::types::atom::nil() {
                return Ok(DefaultValue::Nil);
            }
        }
        if let Ok(b) = val.decode::<bool>() {
            return Ok(DefaultValue::Bool(b));
        }
    }

    if let Ok(i) = val.decode::<i64>() {
        if i >= 0 {
            return Ok(DefaultValue::Uint(i as u64));
        }
        return Ok(DefaultValue::Int(i));
    }

    if let Ok(f) = val.decode::<f64>() {
        return Ok(DefaultValue::Flt(f));
    }

    if let Ok(s) = val.decode::<String>() {
        // Handle special string defaults
        match s.as_str() {
            "{}" => return Ok(DefaultValue::EmptyMap),
            "[]" => return Ok(DefaultValue::EmptyList),
            _ => return Ok(DefaultValue::Str(s)),
        }
    }

    if val.is_map() {
        // Check if it's an empty map
        if let Ok(iter) = val.decode::<Vec<(Term, Term)>>() {
            if iter.is_empty() {
                return Ok(DefaultValue::EmptyMap);
            }
        }
        return Ok(DefaultValue::EmptyMap);
    }

    if val.is_list() {
        if let Ok(list) = val.decode::<Vec<Term>>() {
            if list.is_empty() {
                return Ok(DefaultValue::EmptyList);
            }
        }
        return Ok(DefaultValue::EmptyList);
    }

    Ok(DefaultValue::Nil)
}

fn decode_path_source<'a>(env: Env<'a>, field: Term<'a>) -> Result<PathSource, String> {
    // Check from_output first (resolved to index in decode_fields)
    if let Some(from) = get_string_key(env, field, "from_output")? {
        return Ok(PathSource::FromOutputName(from));
    }

    // Check "paths" (coalesce)
    if let Some(paths_term) = get_term_key(env, field, "paths") {
        if let Ok(paths_list) = paths_term.decode::<Vec<String>>() {
            if !paths_list.is_empty() {
                let mut compiled_paths = Vec::with_capacity(paths_list.len());
                for p in &paths_list {
                    let segments = path::parse(p)
                        .map_err(|e| format!("failed to compile path '{}': {}", p, e))?;
                    compiled_paths.push(segments);
                }
                return Ok(PathSource::Coalesce(compiled_paths));
            }
        }
    }

    // Check "path" (single)
    if let Some(path_str) = get_string_key(env, field, "path")? {
        if path_str == "$" {
            return Ok(PathSource::Root);
        }
        let segments = path::parse(&path_str)
            .map_err(|e| format!("failed to compile path '{}': {}", path_str, e))?;
        return Ok(PathSource::Single(segments));
    }

    // Default to root
    Ok(PathSource::Root)
}

fn decode_transform<'a>(env: Env<'a>, field: Term<'a>) -> Result<Option<FieldTransform>, String> {
    match get_string_key(env, field, "transform")? {
        None => Ok(None),
        Some(s) => match s.to_lowercase().as_str() {
            "upcase" => Ok(Some(FieldTransform::Upcase)),
            "downcase" => Ok(Some(FieldTransform::Downcase)),
            other => Err(format!("unknown transform: {}", other)),
        },
    }
}

fn decode_allowed_values<'a>(env: Env<'a>, map: Term<'a>) -> HashSet<Vec<u8>> {
    match get_term_key(env, map, "allowed_values") {
        Some(t) => t
            .decode::<Vec<String>>()
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.into_bytes())
            .collect(),
        None => HashSet::new(),
    }
}

fn decode_value_map<'a>(env: Env<'a>, field: Term<'a>) -> Result<HashMap<String, i64>, String> {
    let term = match get_term_key(env, field, "value_map") {
        Some(t) if t.is_map() => t,
        _ => return Ok(HashMap::new()),
    };

    decode_string_int_map(env, term)
}

fn decode_pick<'a>(env: Env<'a>, field: Term<'a>) -> Result<Vec<PickEntry>, String> {
    let pick_term = match get_term_key(env, field, "pick") {
        Some(t) => t,
        None => return Ok(vec![]),
    };

    let pick_list: Vec<Term> = pick_term
        .decode()
        .map_err(|_| "pick must be a list".to_string())?;

    let mut entries = Vec::with_capacity(pick_list.len());
    for item in pick_list {
        let key = get_string_key(env, item, "key")?
            .ok_or_else(|| "pick entry missing 'key'".to_string())?;

        let paths_term = get_term_key(env, item, "paths")
            .ok_or_else(|| "pick entry missing 'paths'".to_string())?;

        let paths_list: Vec<String> = paths_term
            .decode()
            .map_err(|_| "pick paths must be a list of strings".to_string())?;

        let mut compiled_paths = Vec::with_capacity(paths_list.len());
        for p in &paths_list {
            let segments = path::parse(p)
                .map_err(|e| format!("failed to compile pick path '{}': {}", p, e))?;
            compiled_paths.push(segments);
        }

        entries.push(PickEntry {
            key,
            paths: compiled_paths,
        });
    }

    Ok(entries)
}

fn decode_string_list_bytes<'a>(env: Env<'a>, map: Term<'a>, key: &str) -> Vec<Vec<u8>> {
    match get_term_key(env, map, key) {
        Some(t) => t
            .decode::<Vec<String>>()
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.into_bytes())
            .collect(),
        None => vec![],
    }
}

fn decode_filter_nil<'a>(env: Env<'a>, field: Term<'a>) -> bool {
    match get_term_key(env, field, "filter_nil") {
        Some(t) => t.decode::<bool>().unwrap_or(false),
        None => false,
    }
}

fn decode_flat_map_value_type<'a>(
    env: Env<'a>,
    field: Term<'a>,
) -> Result<FlatMapValueType, String> {
    match get_string_key(env, field, "value_type")? {
        None => Ok(FlatMapValueType::String),
        Some(s) => match s.to_lowercase().as_str() {
            "string" => Ok(FlatMapValueType::String),
            other => Err(format!(
                "unsupported value_type: '{}' (supported: string)",
                other
            )),
        },
    }
}

// ── Enum8-specific decoders ────────────────────────────────────────────────

pub fn decode_enum8_data<'a>(env: Env<'a>, field: Term<'a>) -> Result<Enum8Data, String> {
    let value_map = decode_enum_values(env, field)?;
    let infer_rules = decode_infer_rules(env, field)?;
    Ok(Enum8Data {
        value_map,
        infer_rules,
    })
}

fn decode_enum_values<'a>(_env: Env<'a>, field: Term<'a>) -> Result<HashMap<String, i8>, String> {
    let term = match get_term_key(_env, field, "enum_values") {
        Some(t) if t.is_map() => t,
        _ => return Ok(HashMap::new()),
    };

    let iter = MapIterator::new(term).ok_or_else(|| "enum_values must be a map".to_string())?;

    let mut result = HashMap::new();
    for (k, v) in iter {
        let key = k
            .decode::<String>()
            .map_err(|_| "enum_values keys must be strings".to_string())?;
        let val = v
            .decode::<i64>()
            .map_err(|_| "enum_values values must be integers".to_string())?;
        // Pre-normalize to lowercase for case-insensitive lookups at map time
        result.insert(key.to_lowercase(), val as i8);
    }
    Ok(result)
}

fn decode_infer_rules<'a>(env: Env<'a>, field: Term<'a>) -> Result<Vec<InferRule>, String> {
    let term = match get_term_key(env, field, "infer") {
        Some(t) => t,
        None => return Ok(vec![]),
    };

    let rule_list: Vec<Term> = term
        .decode()
        .map_err(|_| "infer must be a list".to_string())?;

    let mut rules = Vec::with_capacity(rule_list.len());
    for rule_term in rule_list {
        rules.push(decode_infer_rule(env, rule_term)?);
    }
    Ok(rules)
}

fn decode_infer_rule<'a>(env: Env<'a>, rule: Term<'a>) -> Result<InferRule, String> {
    let result = get_string_key(env, rule, "result")?
        .ok_or_else(|| "infer rule missing 'result'".to_string())?;

    let any = match get_term_key(env, rule, "any") {
        Some(t) => decode_conditions(env, t)?,
        None => vec![],
    };

    let all = match get_term_key(env, rule, "all") {
        Some(t) => decode_conditions(env, t)?,
        None => vec![],
    };

    // Pre-normalize to lowercase for case-insensitive lookup against enum_values
    Ok(InferRule {
        any,
        all,
        result: result.to_lowercase(),
    })
}

fn decode_conditions<'a>(env: Env<'a>, term: Term<'a>) -> Result<Vec<InferCondition>, String> {
    let list: Vec<Term> = term
        .decode()
        .map_err(|_| "conditions must be a list".to_string())?;

    let mut conditions = Vec::with_capacity(list.len());
    for item in list {
        conditions.push(decode_condition(env, item)?);
    }
    Ok(conditions)
}

fn decode_condition<'a>(env: Env<'a>, cond: Term<'a>) -> Result<InferCondition, String> {
    let path_str =
        get_string_key(env, cond, "path")?.ok_or_else(|| "condition missing 'path'".to_string())?;

    let path = path::parse(&path_str)
        .map_err(|e| format!("failed to compile condition path '{}': {}", path_str, e))?;

    let pred_str = get_string_key(env, cond, "predicate")?
        .ok_or_else(|| "condition missing 'predicate'".to_string())?;

    let predicate = parse_predicate(env, cond, &pred_str)?;

    Ok(InferCondition { path, predicate })
}

fn parse_predicate<'a>(env: Env<'a>, cond: Term<'a>, pred_str: &str) -> Result<Predicate, String> {
    match pred_str.to_lowercase().as_str() {
        "exists" => Ok(Predicate::Exists),
        "not_exists" => Ok(Predicate::NotExists),
        "not_zero" => Ok(Predicate::NotZero),
        "is_zero" => Ok(Predicate::IsZero),
        "not_empty" => Ok(Predicate::NotEmpty),
        "is_empty" => Ok(Predicate::IsEmpty),
        "is_string" => Ok(Predicate::IsString),
        "is_number" => Ok(Predicate::IsNumber),
        "is_list" => Ok(Predicate::IsList),
        "is_map" => Ok(Predicate::IsMap),
        "greater_than" => {
            let val = get_comparison_f64(env, cond)?;
            Ok(Predicate::GreaterThan(val))
        }
        "less_than" => {
            let val = get_comparison_f64(env, cond)?;
            Ok(Predicate::LessThan(val))
        }
        "equals" => {
            let val = get_comparison_value(env, cond)?;
            Ok(Predicate::Equals(val))
        }
        "not_equals" => {
            let val = get_comparison_value(env, cond)?;
            Ok(Predicate::NotEquals(val))
        }
        "in" => {
            let vals = get_comparison_values(env, cond)?;
            Ok(Predicate::In(vals))
        }
        other => Err(format!("unknown predicate: {}", other)),
    }
}

fn get_comparison_f64<'a>(env: Env<'a>, cond: Term<'a>) -> Result<f64, String> {
    let term = get_term_key(env, cond, "comparison_value")
        .ok_or_else(|| "predicate requires 'comparison_value'".to_string())?;

    if let Ok(i) = term.decode::<i64>() {
        return Ok(i as f64);
    }
    if let Ok(f) = term.decode::<f64>() {
        return Ok(f);
    }
    if let Ok(s) = term.decode::<String>() {
        return s
            .parse::<f64>()
            .map_err(|_| format!("invalid comparison_value: {}", s));
    }
    Err("comparison_value must be numeric".to_string())
}

fn get_comparison_value<'a>(env: Env<'a>, cond: Term<'a>) -> Result<PredicateValue, String> {
    let term = get_term_key(env, cond, "comparison_value")
        .ok_or_else(|| "predicate requires 'comparison_value'".to_string())?;

    decode_predicate_value(term)
}

fn get_comparison_values<'a>(env: Env<'a>, cond: Term<'a>) -> Result<Vec<PredicateValue>, String> {
    let term = get_term_key(env, cond, "comparison_values")
        .ok_or_else(|| "predicate requires 'comparison_values'".to_string())?;

    let list: Vec<Term> = term
        .decode()
        .map_err(|_| "comparison_values must be a list".to_string())?;

    let mut vals = Vec::with_capacity(list.len());
    for item in list {
        vals.push(decode_predicate_value(item)?);
    }
    Ok(vals)
}

fn decode_predicate_value(term: Term) -> Result<PredicateValue, String> {
    if let Ok(b) = term.decode::<bool>() {
        return Ok(PredicateValue::Bool(b));
    }
    if let Ok(i) = term.decode::<i64>() {
        return Ok(PredicateValue::Int(i));
    }
    if let Ok(f) = term.decode::<f64>() {
        return Ok(PredicateValue::Flt(f));
    }
    if let Ok(s) = term.decode::<String>() {
        return Ok(PredicateValue::Str(s));
    }
    Err("comparison value must be string, integer, float, or boolean".to_string())
}

fn decode_string_int_map<'a>(
    _env: Env<'a>,
    term: Term<'a>,
) -> Result<HashMap<String, i64>, String> {
    let iter = MapIterator::new(term).ok_or_else(|| "value_map must be a map".to_string())?;

    let mut result = HashMap::new();
    for (k, v) in iter {
        let key = k
            .decode::<String>()
            .map_err(|_| "value_map keys must be strings".to_string())?;
        let val = v
            .decode::<i64>()
            .map_err(|_| "value_map values must be integers".to_string())?;
        // Pre-normalize to lowercase for case-insensitive lookups at map time
        result.insert(key.to_lowercase(), val);
    }
    Ok(result)
}

// ── Helper functions for reading Elixir map keys ───────────────────────────

pub fn get_term_key<'a>(env: Env<'a>, map: Term<'a>, key: &str) -> Option<Term<'a>> {
    let str_key = key.encode(env);
    if let Ok(val) = map.map_get(str_key) {
        if val.is_atom() {
            if let Ok(a) = rustler::types::atom::Atom::from_term(val) {
                if a == rustler::types::atom::nil() {
                    return None;
                }
            }
        }
        return Some(val);
    }

    // Try atom key
    if let Ok(atom) = rustler::types::atom::Atom::from_str(env, key) {
        if let Ok(val) = map.map_get(atom.encode(env)) {
            if val.is_atom() {
                if let Ok(a) = rustler::types::atom::Atom::from_term(val) {
                    if a == rustler::types::atom::nil() {
                        return None;
                    }
                }
            }
            return Some(val);
        }
    }

    None
}

pub fn get_string_key<'a>(
    env: Env<'a>,
    map: Term<'a>,
    key: &str,
) -> Result<Option<String>, String> {
    match get_term_key(env, map, key) {
        Some(t) => {
            if let Ok(s) = t.decode::<String>() {
                Ok(Some(s))
            } else if t.is_atom() {
                if let Ok(s) = t.atom_to_string() {
                    Ok(Some(s))
                } else {
                    Ok(None)
                }
            } else {
                Ok(None)
            }
        }
        None => Ok(None),
    }
}

pub fn get_int_key<'a>(env: Env<'a>, map: Term<'a>, key: &str) -> Option<i64> {
    get_term_key(env, map, key).and_then(|t| t.decode::<i64>().ok())
}
