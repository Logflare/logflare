//! Derived-field rules shared by every mapper output writer.
//!
//! Each rule is a pure function over already-decoded values so that
//! RowBinary, NDJSON, and any future format resolve derived columns
//! identically.

/// OTEL `SeverityNumber` defines 1-24; 0 is UNSPECIFIED. `uint8` coercion saturates
/// anything larger to 255, so a supplied value outside this range is not a severity
/// and the `severity_text` mapping is used instead. Non-integer inputs (floats,
/// booleans) never reach this check: `severity_number_alt` is configured with
/// `coercion: :strict`, which resolves them to the field default of 0.
const OTEL_SEVERITY_MIN: u64 = 1;
const OTEL_SEVERITY_MAX: u64 = 24;

/// Log `severity_number`: the alternate source wins when it carries a value.
pub fn severity_number(severity_alt: u64, mapped: u64) -> u64 {
    if (OTEL_SEVERITY_MIN..=OTEL_SEVERITY_MAX).contains(&severity_alt) {
        severity_alt
    } else {
        mapped
    }
}

/// Trace `duration` emitted in `duration_precision` (0..=9), the precision
/// of the `start_time`/`end_time` fields. `explicit` is the mapped `duration`
/// field in OTEL nanoseconds; a non-zero value wins and is scaled down.
/// Otherwise the span is `end_time - start_time`, whose endpoints were
/// already coerced to that precision, when both decode and the span is
/// positive.
pub fn duration<E>(
    explicit: u64,
    start_time: Result<i64, E>,
    end_time: Result<i64, E>,
    duration_precision: u8,
) -> u64 {
    if explicit != 0 {
        return explicit / 10u64.pow(u32::from(9_u8.saturating_sub(duration_precision)));
    }
    match (start_time, end_time) {
        (Ok(start), Ok(end)) if end > start => end.abs_diff(start),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn severity_prefers_alt_when_set() {
        assert_eq!(severity_number(5, 9), 5);
        assert_eq!(severity_number(0, 9), 9);
        assert_eq!(severity_number(0, 0), 0);
    }

    #[test]
    fn duration_passes_through_non_zero() {
        assert_eq!(duration::<()>(42, Ok(100), Ok(50), 9), 42);
    }

    #[test]
    fn duration_scales_explicit_nanoseconds_to_precision() {
        assert_eq!(duration::<()>(42_000, Ok(0), Ok(0), 6), 42);
        assert_eq!(duration::<()>(999, Ok(0), Ok(1_000), 6), 0);
        assert_eq!(duration::<()>(u64::MAX, Ok(0), Ok(0), 6), u64::MAX / 1_000);
    }

    #[test]
    fn duration_derives_from_positive_span() {
        assert_eq!(duration::<()>(0, Ok(100), Ok(250), 9), 150);
    }

    #[test]
    fn duration_is_zero_for_non_positive_span() {
        assert_eq!(duration::<()>(0, Ok(100), Ok(100), 9), 0);
        assert_eq!(duration::<()>(0, Ok(100), Ok(50), 9), 0);
    }

    #[test]
    fn duration_is_zero_when_endpoints_missing() {
        assert_eq!(duration(0, Err(()), Ok(50), 9), 0);
        assert_eq!(duration(0, Ok(50), Err(()), 9), 0);
        assert_eq!(duration::<()>(0, Err(()), Err(()), 9), 0);
    }

    #[test]
    fn duration_handles_extreme_spans() {
        assert_eq!(duration::<()>(0, Ok(i64::MIN), Ok(i64::MAX), 9), u64::MAX);
    }
}
