use chrono::DateTime as ChronoDateTime;
use rustler::{Encoder, Env, Term};

use crate::mapping::{DefaultValue, FieldTransform, FieldType};

mod atoms {
    rustler::atoms! {
        nil,
        true_ = "true",
        false_ = "false",
    }
}

/// Coerce a BEAM term to the target field type.
pub fn coerce<'a>(env: Env<'a>, value: Term<'a>, field_type: &FieldType) -> Term<'a> {
    let nil = atoms::nil().encode(env);

    if value == nil {
        // For numeric types, nil coerces to their zero value.
        // DateTime64 is intentionally excluded — nil flows through so the
        // Elixir side can substitute the event's real timestamp instead of
        // silently inserting epoch time (1970-01-01).
        return match field_type {
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
        FieldType::Json => value, // pass-through
    }
}

/// Apply a transform to a resolved string value.
pub fn apply_transform<'a>(env: Env<'a>, value: Term<'a>, transform: &FieldTransform) -> Term<'a> {
    let nil = atoms::nil().encode(env);
    if value == nil {
        return nil;
    }

    if let Ok(s) = value.decode::<String>() {
        match transform {
            FieldTransform::Upcase => s.to_uppercase().encode(env),
            FieldTransform::Downcase => s.to_lowercase().encode(env),
        }
    } else {
        value
    }
}

/// Encode a default value to a BEAM term.
pub fn encode_default<'a>(env: Env<'a>, default: &DefaultValue) -> Term<'a> {
    match default {
        DefaultValue::Nil => atoms::nil().encode(env),
        DefaultValue::Str(s) => s.encode(env),
        DefaultValue::Int(i) => i.encode(env),
        DefaultValue::Uint(u) => u.encode(env),
        DefaultValue::Flt(f) => f.encode(env),
        DefaultValue::Bool(b) => b.encode(env),
        DefaultValue::EmptyList => Vec::<Term>::new().encode(env),
        DefaultValue::EmptyMap => Term::map_new(env),
    }
}

/// Look up a string value in a value_map, returning the mapped integer.
pub fn apply_value_map<'a>(env: Env<'a>, value: Term<'a>, map: &[(String, i64)]) -> Term<'a> {
    let nil = atoms::nil().encode(env);
    if value == nil || map.is_empty() {
        return nil;
    }

    if let Ok(s) = value.decode::<String>() {
        let s_upper = s.to_uppercase();
        for (key, val) in map {
            if key.to_uppercase() == s_upper {
                return val.encode(env);
            }
        }
    }

    nil
}

// ── Private coercion functions ─────────────────────────────────────────────

fn coerce_string<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    if value.is_binary() {
        return value;
    }

    if let Ok(i) = value.decode::<i64>() {
        return i.to_string().encode(env);
    }

    if let Ok(f) = value.decode::<f64>() {
        return f.to_string().encode(env);
    }

    if let Ok(b) = value.decode::<bool>() {
        return if b { "true" } else { "false" }.encode(env);
    }

    // For atoms (non-bool), try to get string representation
    if value.is_atom() {
        if let Ok(s) = value.atom_to_string() {
            return s.encode(env);
        }
    }

    "".encode(env)
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

    if let Ok(s) = value.decode::<String>() {
        if let Ok(u) = s.parse::<u64>() {
            return u.min(max).encode(env);
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

    if let Ok(s) = value.decode::<String>() {
        if let Ok(i) = s.parse::<i32>() {
            return i.encode(env);
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

    if let Ok(s) = value.decode::<String>() {
        if let Ok(f) = s.parse::<f64>() {
            return f.encode(env);
        }
    }

    0.0f64.encode(env)
}

fn coerce_bool<'a>(env: Env<'a>, value: Term<'a>) -> Term<'a> {
    if let Ok(b) = value.decode::<bool>() {
        return b.encode(env);
    }

    if let Ok(s) = value.decode::<String>() {
        let lower = s.to_lowercase();
        return (lower == "true" || lower == "1").encode(env);
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

    if let Ok(s) = value.decode::<String>() {
        if let Ok(dt) = ChronoDateTime::parse_from_rfc3339(&s) {
            if let Some(nanos) = dt.timestamp_nanos_opt() {
                return scale(nanos, 9, precision).encode(env);
            }
        }
        // Try ISO8601 with space separator (e.g., "2026-01-21 17:54:48.144506Z")
        let rfc3339_attempt = s.replace(' ', "T");
        if let Ok(dt) = ChronoDateTime::parse_from_rfc3339(&rfc3339_attempt) {
            if let Some(nanos) = dt.timestamp_nanos_opt() {
                return scale(nanos, 9, precision).encode(env);
            }
        }
    }

    // Return 0 for unparseable values
    0i64.encode(env)
}

/// Detect the precision of a unix timestamp by its digit count.
fn detect_precision(value: i64) -> u8 {
    let digits = count_digits(value.unsigned_abs());
    match digits {
        0..=10 => 0,  // seconds
        11..=13 => 3, // milliseconds
        14..=16 => 6, // microseconds
        _ => 9,       // nanoseconds
    }
}

/// Scale a timestamp value from one precision to another.
fn scale(value: i64, from: u8, to: u8) -> i64 {
    match from.cmp(&to) {
        std::cmp::Ordering::Equal => value,
        std::cmp::Ordering::Less => value.saturating_mul(10_i64.pow((to - from) as u32)),
        std::cmp::Ordering::Greater => value / 10_i64.pow((from - to) as u32),
    }
}

/// Count the number of decimal digits in a u64.
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
        assert_eq!(detect_precision(-1769018088144506), 6); // negative microseconds
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
