use std::collections::HashMap;

use chrono::DateTime as ChronoDateTime;
use rustler::types::list::ListIterator;
use rustler::{Binary, Encoder, Env, NewBinary, Term};

use crate::mapping::{DefaultValue, FieldTransform, FieldType};

/// Case-insensitive lookup using stack-allocated buffer for ASCII values.
/// Falls back to heap allocation for values > 128 bytes or non-ASCII.
pub fn case_insensitive_get<'b, V>(map: &'b HashMap<String, V>, term: Term<'_>) -> Option<&'b V> {
    let b = term.decode::<Binary>().ok()?;
    let bytes = b.as_slice();

    // Fast path: stack-allocated ASCII lowercase for typical field values
    if bytes.len() <= 128 && bytes.is_ascii() {
        let mut buf = [0u8; 128];
        let len = bytes.len();
        for (i, byte) in bytes.iter().enumerate() {
            buf[i] = byte.to_ascii_lowercase();
        }
        // Safety: we confirmed all bytes are ASCII, so lowercase is valid UTF-8
        let lowered = std::str::from_utf8(&buf[..len]).ok()?;
        map.get(lowered)
    } else {
        // Fallback for non-ASCII or very long strings
        let s = std::str::from_utf8(bytes).ok()?;
        map.get(&s.to_lowercase())
    }
}

/// Coerce a BEAM term to the target field type.
#[inline]
pub fn coerce<'a>(
    env: Env<'a>,
    value: Term<'a>,
    field_type: &FieldType,
    nil: Term<'a>,
) -> Term<'a> {
    if value == nil {
        // For numeric types, nil coerces to their zero value.
        // DateTime64 is intentionally excluded — nil flows through so the
        // Elixir side can substitute the event's real timestamp instead of
        // silently inserting epoch time (1970-01-01).
        return match field_type {
            FieldType::String => crate::encode_string(env, ""),
            FieldType::UInt8 | FieldType::UInt32 | FieldType::UInt64 => 0u64.encode(env),
            FieldType::Int32 => 0i32.encode(env),
            FieldType::Float64 => 0.0f64.encode(env),
            FieldType::Bool => false.encode(env),
            FieldType::Enum8 { .. } => 0i8.encode(env),
            _ => nil,
        };
    }

    match field_type {
        FieldType::String => coerce_string(env, value),
        FieldType::UInt8 => coerce_uint(env, value, u8::MAX as u64),
        FieldType::UInt32 => coerce_uint(env, value, u32::MAX as u64),
        FieldType::UInt64 => coerce_uint(env, value, u64::MAX),
        FieldType::Int32 => coerce_int32(env, value),
        FieldType::Float64 => coerce_float64(env, value),
        FieldType::Bool => coerce_bool(env, value),
        FieldType::Enum8 { .. } => coerce_enum8(env, value),
        FieldType::DateTime64 { precision } => coerce_datetime64(env, value, *precision),
        FieldType::Json | FieldType::FlatMap => value, // pass-through
        // Array types are handled by coerce_array, not coerce
        FieldType::ArrayString
        | FieldType::ArrayUInt64
        | FieldType::ArrayFloat64
        | FieldType::ArrayDateTime64 { .. }
        | FieldType::ArrayJson
        | FieldType::ArrayMap
        | FieldType::ArrayFlatMap => Vec::<Term>::new().encode(env),
    }
}

/// Apply a transform to a resolved string value.
#[inline]
pub fn apply_transform<'a>(
    env: Env<'a>,
    value: Term<'a>,
    transform: &FieldTransform,
    nil: Term<'a>,
) -> Term<'a> {
    if value == nil {
        return nil;
    }

    if let Ok(binary) = value.decode::<Binary>() {
        let bytes = binary.as_slice();
        if bytes.is_ascii() {
            let unchanged = match transform {
                FieldTransform::Upcase => !bytes.iter().any(u8::is_ascii_lowercase),
                FieldTransform::Downcase => !bytes.iter().any(u8::is_ascii_uppercase),
            };
            if unchanged {
                return value;
            }

            let mut transformed = NewBinary::new(env, bytes.len());
            for (output, input) in transformed.as_mut_slice().iter_mut().zip(bytes) {
                *output = match transform {
                    FieldTransform::Upcase => input.to_ascii_uppercase(),
                    FieldTransform::Downcase => input.to_ascii_lowercase(),
                };
            }
            return transformed.into();
        }
    }

    if let Ok(s) = value.decode::<String>() {
        match transform {
            FieldTransform::Upcase => crate::encode_string(env, &s.to_uppercase()),
            FieldTransform::Downcase => crate::encode_string(env, &s.to_lowercase()),
        }
    } else {
        value
    }
}

/// Encode a default value to a BEAM term.
#[inline]
pub fn encode_default<'a>(env: Env<'a>, default: &DefaultValue, nil: Term<'a>) -> Term<'a> {
    match default {
        DefaultValue::Nil => nil,
        DefaultValue::Str(s) => crate::encode_string(env, s),
        DefaultValue::Int(i) => i.encode(env),
        DefaultValue::Uint(u) => u.encode(env),
        DefaultValue::Flt(f) => f.encode(env),
        DefaultValue::Bool(b) => b.encode(env),
        DefaultValue::EmptyList => Vec::<Term>::new().encode(env),
        DefaultValue::EmptyMap => Term::map_new(env),
    }
}

/// Look up a string value in a value_map, returning the mapped integer.
pub fn apply_value_map<'a>(
    env: Env<'a>,
    value: Term<'a>,
    map: &HashMap<String, i64>,
    nil: Term<'a>,
) -> Term<'a> {
    if value == nil || map.is_empty() {
        return nil;
    }

    // Keys are pre-lowercased at compile time
    if let Some(val) = case_insensitive_get(map, value) {
        return val.encode(env);
    }

    nil
}

/// Look up a string value in a string value_map, returning the mapped string.
///
/// Returns `nil` when the value is absent from the map; the caller substitutes
/// the field's default in that case.
pub fn apply_value_map_str<'a>(
    env: Env<'a>,
    value: Term<'a>,
    map: &HashMap<String, String>,
    nil: Term<'a>,
) -> Term<'a> {
    if value == nil || map.is_empty() {
        return nil;
    }

    // Keys are pre-lowercased at compile time
    if let Some(val) = case_insensitive_get(map, value) {
        return crate::encode_string(env, val);
    }

    nil
}

/// Coerce a BEAM term to an array of the target element type.
///
/// If the value is not a list, returns an empty list.
/// Each element is coerced according to the array's inner type.
/// nil elements are either filtered (filter_nil=true) or coerced to the
/// inner type's zero value (filter_nil=false).
pub fn coerce_array<'a>(
    env: Env<'a>,
    value: Term<'a>,
    field_type: &FieldType,
    filter_nil: bool,
    nil: Term<'a>,
) -> Term<'a> {
    // If value is nil or not a list, return empty list
    if value == nil {
        return Vec::<Term>::new().encode(env);
    }

    let iter = match value.decode::<ListIterator>() {
        Ok(iter) => iter,
        Err(_) => return Vec::<Term>::new().encode(env),
    };

    let inner_type = array_inner_type(field_type);
    let mut result: Vec<Term<'a>> = Vec::new();

    for elem in iter {
        if let Some(elem) =
            coerce_array_element(env, elem, field_type, inner_type.as_ref(), filter_nil, nil)
        {
            result.push(elem);
        }
    }

    result.encode(env)
}

#[inline]
pub fn coerce_array_element<'a>(
    env: Env<'a>,
    elem: Term<'a>,
    field_type: &FieldType,
    inner_type: Option<&FieldType>,
    filter_nil: bool,
    nil: Term<'a>,
) -> Option<Term<'a>> {
    if elem == nil {
        return if filter_nil {
            None
        } else {
            Some(array_nil_value(env, field_type))
        };
    }

    match field_type {
        FieldType::ArrayFlatMap => elem
            .is_map()
            .then(|| crate::mapper::flatten_and_stringify(env, elem, nil)),
        FieldType::ArrayMap => elem.is_map().then_some(elem),
        FieldType::ArrayJson => Some(elem),
        _ => Some(match inner_type {
            Some(inner) => coerce(env, elem, inner, nil),
            None => elem,
        }),
    }
}

/// Map array field types to their corresponding scalar inner type for coercion.
#[inline]
pub fn array_inner_type(field_type: &FieldType) -> Option<FieldType> {
    match field_type {
        FieldType::ArrayString => Some(FieldType::String),
        FieldType::ArrayUInt64 => Some(FieldType::UInt64),
        FieldType::ArrayFloat64 => Some(FieldType::Float64),
        FieldType::ArrayDateTime64 { precision } => Some(FieldType::DateTime64 {
            precision: *precision,
        }),
        _ => None,
    }
}

/// Return the zero/nil coercion value for an array element based on the array type.
fn array_nil_value<'a>(env: Env<'a>, field_type: &FieldType) -> Term<'a> {
    match field_type {
        FieldType::ArrayString => crate::encode_string(env, ""),
        FieldType::ArrayUInt64 => 0u64.encode(env),
        FieldType::ArrayFloat64 => 0.0f64.encode(env),
        FieldType::ArrayDateTime64 { .. } => 0i64.encode(env),
        FieldType::ArrayJson => Term::map_new(env),
        FieldType::ArrayMap => Term::map_new(env),
        FieldType::ArrayFlatMap => Term::map_new(env),
        _ => 0u64.encode(env),
    }
}

// ── Private coercion functions ─────────────────────────────────────────────

fn coerce_string<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    if value.is_binary() {
        return value;
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

    // For atoms (non-bool), try to get string representation
    if value.is_atom() {
        if let Ok(s) = value.atom_to_string() {
            return crate::encode_string(env, &s);
        }
    }

    crate::encode_string(env, "")
}

fn coerce_uint<'a>(env: Env<'a>, value: Term<'a>, max: u64) -> Term<'a> {
    if let Ok(i) = value.decode::<i64>() {
        if i < 0 {
            return 0u64.encode(env);
        }
        let u = i as u64;
        return u.min(max).encode(env);
    }

    if let Ok(f) = value.decode::<f64>() {
        if f < 0.0 {
            return 0u64.encode(env);
        }
        let u = f as u64;
        return u.min(max).encode(env);
    }

    if let Ok(binary) = value.decode::<Binary>() {
        if let Ok(s) = std::str::from_utf8(binary.as_slice()) {
            if let Ok(u) = s.parse::<u64>() {
                return u.min(max).encode(env);
            }
        }
    }

    if let Ok(b) = value.decode::<bool>() {
        return if b { 1u64 } else { 0u64 }.encode(env);
    }

    0u64.encode(env)
}

fn coerce_int32<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    if let Ok(i) = value.decode::<i64>() {
        let clamped = i.max(i32::MIN as i64).min(i32::MAX as i64);
        return (clamped as i32).encode(env);
    }

    if let Ok(f) = value.decode::<f64>() {
        let clamped = f.max(i32::MIN as f64).min(i32::MAX as f64);
        return (clamped as i32).encode(env);
    }

    if let Ok(binary) = value.decode::<Binary>() {
        if let Ok(s) = std::str::from_utf8(binary.as_slice()) {
            if let Ok(i) = s.parse::<i32>() {
                return i.encode(env);
            }
        }
    }

    0i32.encode(env)
}

fn coerce_float64<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    if let Ok(f) = value.decode::<f64>() {
        return f.encode(env);
    }

    if let Ok(i) = value.decode::<i64>() {
        return (i as f64).encode(env);
    }

    if let Ok(binary) = value.decode::<Binary>() {
        if let Ok(s) = std::str::from_utf8(binary.as_slice()) {
            if let Ok(f) = s.parse::<f64>() {
                return f.encode(env);
            }
        }
    }

    0.0f64.encode(env)
}

fn coerce_bool<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    if let Ok(b) = value.decode::<bool>() {
        return b.encode(env);
    }

    if let Ok(binary) = value.decode::<Binary>() {
        let bytes = binary.as_slice();
        return (bytes.eq_ignore_ascii_case(b"true") || bytes == b"1").encode(env);
    }

    if let Ok(i) = value.decode::<i64>() {
        return (i != 0).encode(env);
    }

    false.encode(env)
}

fn coerce_enum8<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    // Enum8 values should already be resolved to integers by the mapper
    if let Ok(i) = value.decode::<i64>() {
        return (i as i8).encode(env);
    }

    0i8.encode(env)
}

fn coerce_datetime64<'a>(env: Env<'a>, value: Term<'a>, precision: u8) -> Term<'a> {
    if let Ok(i) = value.decode::<i64>() {
        let source_precision = detect_precision(i);
        return scale(i, source_precision, precision).encode(env);
    }

    if let Ok(binary) = value.decode::<Binary>() {
        if let Ok(s) = std::str::from_utf8(binary.as_slice()) {
            if let Ok(dt) = ChronoDateTime::parse_from_rfc3339(s) {
                if let Some(nanos) = dt.timestamp_nanos_opt() {
                    return scale(nanos, 9, precision).encode(env);
                }
            }
            // Try ISO8601 with space separator (e.g., "2026-01-21 17:54:48.144506Z")
            if s.contains(' ') {
                let rfc3339_attempt = s.replace(' ', "T");
                if let Ok(dt) = ChronoDateTime::parse_from_rfc3339(&rfc3339_attempt) {
                    if let Some(nanos) = dt.timestamp_nanos_opt() {
                        return scale(nanos, 9, precision).encode(env);
                    }
                }
            }
        }
    }

    // Return 0 for unparseable values
    0i64.encode(env)
}

/// Detect the precision of a unix timestamp by its digit count.
fn detect_precision(value: i64) -> u8 {
    match value.unsigned_abs() {
        0..=9_999_999_999 => 0,
        10_000_000_000..=9_999_999_999_999 => 3,
        10_000_000_000_000..=9_999_999_999_999_999 => 6,
        _ => 9,
    }
}

/// Scale a timestamp value from one precision to another.
fn scale(value: i64, from: u8, to: u8) -> i64 {
    match from.cmp(&to) {
        std::cmp::Ordering::Equal => value,
        std::cmp::Ordering::Less => value.saturating_mul(decimal_scale(to - from)),
        std::cmp::Ordering::Greater => value / decimal_scale(from - to),
    }
}

#[inline]
fn decimal_scale(precision: u8) -> i64 {
    match precision {
        0 => 1,
        3 => 1_000,
        6 => 1_000_000,
        9 => 1_000_000_000,
        other => 10_i64.pow(other as u32),
    }
}

/// Count the number of decimal digits in a u64.
#[cfg(test)]
fn count_digits(mut n: u64) -> u32 {
    if n == 0 {
        return 1;
    }
    let mut count = 0u32;
    while n > 0 {
        count += 1;
        n /= 10;
    }
    count
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_digits() {
        assert_eq!(count_digits(0), 1);
        assert_eq!(count_digits(1), 1);
        assert_eq!(count_digits(9), 1);
        assert_eq!(count_digits(10), 2);
        assert_eq!(count_digits(99), 2);
        assert_eq!(count_digits(100), 3);
        assert_eq!(count_digits(1769018088), 10); // seconds
        assert_eq!(count_digits(1769018088144), 13); // milliseconds
        assert_eq!(count_digits(1769018088144506), 16); // microseconds
        assert_eq!(count_digits(1769018088144506000), 19); // nanoseconds
    }

    #[test]
    fn test_detect_precision() {
        assert_eq!(detect_precision(1769018088), 0); // seconds
        assert_eq!(detect_precision(1769018088144), 3); // milliseconds
        assert_eq!(detect_precision(1769018088144506), 6); // microseconds
        assert_eq!(detect_precision(1769018088144506000), 9); // nanoseconds
        assert_eq!(detect_precision(0), 0);
        assert_eq!(detect_precision(9_999_999_999), 0);
        assert_eq!(detect_precision(10_000_000_000), 3);
        assert_eq!(detect_precision(9_999_999_999_999), 3);
        assert_eq!(detect_precision(10_000_000_000_000), 6);
        assert_eq!(detect_precision(9_999_999_999_999_999), 6);
        assert_eq!(detect_precision(10_000_000_000_000_000), 9);
        assert_eq!(detect_precision(-1769018088144506), 6); // negative microseconds
        assert_eq!(detect_precision(i64::MIN), 9);
    }

    #[test]
    fn test_scale() {
        // Same precision -> no change
        assert_eq!(scale(1000, 3, 3), 1000);

        // Scale up: seconds to nanoseconds
        assert_eq!(scale(1, 0, 9), 1_000_000_000);

        // Scale up: milliseconds to nanoseconds
        assert_eq!(scale(1000, 3, 9), 1_000_000_000);

        // Scale up: microseconds to nanoseconds
        assert_eq!(scale(1000000, 6, 9), 1_000_000_000);

        // Scale down: nanoseconds to seconds
        assert_eq!(scale(1_000_000_000, 9, 0), 1);

        // Scale down: nanoseconds to milliseconds
        assert_eq!(scale(1_000_000_000, 9, 3), 1_000);

        // Scale down: nanoseconds to microseconds
        assert_eq!(scale(1_000_000_000, 9, 6), 1_000_000);

        // Real timestamp: microseconds to nanoseconds
        assert_eq!(scale(1769018088144506, 6, 9), 1769018088144506000);
    }
}
