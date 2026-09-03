//! Derived-field rules shared by every mapper output writer.
//!
//! Each rule is a pure function over already-decoded values so that
//! RowBinary, NDJSON, and any future format resolve derived columns
//! identically.

/// Log `severity_number`: the alternate source wins when it carries a value.
pub fn severity_number(severity_alt: u64, mapped: u64) -> u64 {
    if severity_alt > 0 {
        severity_alt
    } else {
        mapped
    }
}

/// Trace `duration`: an explicit non-zero value passes through; otherwise it is
/// computed from `end_time - start_time` when both decode and the span is
/// positive.
pub fn duration<E>(duration: u64, start_time: Result<i64, E>, end_time: Result<i64, E>) -> u64 {
    if duration != 0 {
        return duration;
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
        assert_eq!(duration::<()>(42, Ok(100), Ok(50)), 42);
    }

    #[test]
    fn duration_derives_from_positive_span() {
        assert_eq!(duration::<()>(0, Ok(100), Ok(250)), 150);
    }

    #[test]
    fn duration_is_zero_for_non_positive_span() {
        assert_eq!(duration::<()>(0, Ok(100), Ok(100)), 0);
        assert_eq!(duration::<()>(0, Ok(100), Ok(50)), 0);
    }

    #[test]
    fn duration_is_zero_when_endpoints_missing() {
        assert_eq!(duration(0, Err(()), Ok(50)), 0);
        assert_eq!(duration(0, Ok(50), Err(())), 0);
        assert_eq!(duration::<()>(0, Err(()), Err(())), 0);
    }

    #[test]
    fn duration_handles_extreme_spans() {
        assert_eq!(duration::<()>(0, Ok(i64::MIN), Ok(i64::MAX)), u64::MAX);
    }
}
