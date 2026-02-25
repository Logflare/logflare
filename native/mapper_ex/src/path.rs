/// Path segment types for JSONPath evaluation.
///
/// Supports:
/// - Simple keys: `$.firstName` -> `[Key("firstName")]`
/// - Nested keys: `$.address.zip` -> `[Key("address"), Key("zip")]`
/// - Array wildcard: `$.items[*]` -> `[Key("items"), Wildcard]`
/// - Array index: `$.source[0]` -> `[Key("source"), Index(0)]`
/// - Combined: `$.notes[*].action` -> `[Key("notes"), Wildcard, Key("action")]`
#[derive(Debug, Clone)]
pub enum PathSegment {
    Key(String),
    Wildcard,
    Index(usize),
}

/// Parses a JSONPath string into a vector of PathSegments.
///
/// Expects paths starting with `$` or `$.`.
/// Returns an error string if the path is malformed.
pub fn parse(path: &str) -> Result<Vec<PathSegment>, String> {
    let rest = if path == "$" {
        return Ok(vec![]);
    } else if let Some(rest) = path.strip_prefix("$.") {
        rest
    } else if let Some(rest) = path.strip_prefix('$') {
        rest
    } else {
        return Err(format!("path must start with '$': {path}"));
    };

    if rest.is_empty() {
        return Ok(vec![]);
    }

    let mut segments = Vec::new();
    let mut chars = rest.chars().peekable();
    let mut key_buf = String::new();

    while let Some(&ch) = chars.peek() {
        match ch {
            '.' => {
                if !key_buf.is_empty() {
                    segments.push(PathSegment::Key(key_buf.clone()));
                    key_buf.clear();
                }
                chars.next();
            }
            '[' => {
                if !key_buf.is_empty() {
                    segments.push(PathSegment::Key(key_buf.clone()));
                    key_buf.clear();
                }
                chars.next(); // consume '['

                let mut bracket_buf = String::new();
                loop {
                    match chars.next() {
                        Some(']') => break,
                        Some(c) => bracket_buf.push(c),
                        None => return Err(format!("unclosed bracket in path: {path}")),
                    }
                }

                if bracket_buf == "*" {
                    segments.push(PathSegment::Wildcard);
                } else {
                    match bracket_buf.parse::<usize>() {
                        Ok(idx) => segments.push(PathSegment::Index(idx)),
                        Err(_) => {
                            return Err(format!(
                                "invalid bracket expression '[{bracket_buf}]' in path: {path}"
                            ))
                        }
                    }
                }
            }
            _ => {
                key_buf.push(ch);
                chars.next();
            }
        }
    }

    if !key_buf.is_empty() {
        segments.push(PathSegment::Key(key_buf));
    }

    Ok(segments)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_key() {
        let segs = parse("$.firstName").unwrap();
        assert_eq!(segs.len(), 1);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "firstName"));
    }

    #[test]
    fn test_nested_keys() {
        let segs = parse("$.address.zip").unwrap();
        assert_eq!(segs.len(), 2);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "address"));
        assert!(matches!(&segs[1], PathSegment::Key(k) if k == "zip"));
    }

    #[test]
    fn test_wildcard() {
        let segs = parse("$.educations[*]").unwrap();
        assert_eq!(segs.len(), 2);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "educations"));
        assert!(matches!(&segs[1], PathSegment::Wildcard));
    }

    #[test]
    fn test_wildcard_with_nested() {
        let segs = parse("$.notes[*].action").unwrap();
        assert_eq!(segs.len(), 3);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "notes"));
        assert!(matches!(&segs[1], PathSegment::Wildcard));
        assert!(matches!(&segs[2], PathSegment::Key(k) if k == "action"));
    }

    #[test]
    fn test_index() {
        let segs = parse("$.source[0]").unwrap();
        assert_eq!(segs.len(), 2);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "source"));
        assert!(matches!(&segs[1], PathSegment::Index(0)));
    }

    #[test]
    fn test_complex_combined() {
        let segs = parse("$.notes[*].candidates.data[0].id").unwrap();
        assert_eq!(segs.len(), 6);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "notes"));
        assert!(matches!(&segs[1], PathSegment::Wildcard));
        assert!(matches!(&segs[2], PathSegment::Key(k) if k == "candidates"));
        assert!(matches!(&segs[3], PathSegment::Key(k) if k == "data"));
        assert!(matches!(&segs[4], PathSegment::Index(0)));
        assert!(matches!(&segs[5], PathSegment::Key(k) if k == "id"));
    }

    #[test]
    fn test_root_only() {
        let segs = parse("$").unwrap();
        assert!(segs.is_empty());
    }

    #[test]
    fn test_root_dot() {
        let segs = parse("$.").unwrap();
        assert!(segs.is_empty());
    }

    #[test]
    fn test_invalid_no_dollar() {
        assert!(parse("firstName").is_err());
    }

    #[test]
    fn test_deep_nesting() {
        let segs = parse("$.a.b.c.d.e").unwrap();
        assert_eq!(segs.len(), 5);
        assert!(matches!(&segs[0], PathSegment::Key(k) if k == "a"));
        assert!(matches!(&segs[4], PathSegment::Key(k) if k == "e"));
    }
}
