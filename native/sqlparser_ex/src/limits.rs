use sqlparser::ast::Statement;
use sqlparser::dialect::Dialect;
use sqlparser::parser::ParserError;
use sqlparser::tokenizer::{Token, TokenWithSpan, Tokenizer};
use std::io::{self, Write};

const MAX_SIGNIFICANT_TOKENS: usize = 20_000;
const MAX_AST_JSON_BYTES: usize = 16 * 1024 * 1024;
const JSON_RECURSION_BUDGET: usize = 128;

pub fn tokenize(dialect: &dyn Dialect, query: &str) -> Result<Vec<TokenWithSpan>, ParserError> {
    let tokens = Tokenizer::new(dialect, query).tokenize_with_location()?;
    let count = tokens
        .iter()
        .filter(|token| !matches!(token.token, Token::Whitespace(_)))
        .take(MAX_SIGNIFICANT_TOKENS + 1)
        .count();
    if count > MAX_SIGNIFICANT_TOKENS {
        return Err(ParserError::ParserError(format!(
            "Query is too complex to parse (limit {MAX_SIGNIFICANT_TOKENS} significant tokens)."
        )));
    }
    Ok(tokens)
}

pub fn encode(statements: &[Statement]) -> Result<String, String> {
    let mut writer = LimitedWriter::new(MAX_AST_JSON_BYTES);
    serde_json::to_writer(&mut writer, statements).map_err(|e| e.to_string())?;
    check_json_depth(&writer.bytes)?;
    String::from_utf8(writer.bytes).map_err(|e| e.to_string())
}

struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl LimitedWriter {
    fn new(limit: usize) -> Self {
        Self {
            bytes: Vec::new(),
            limit,
        }
    }
}

impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit - self.bytes.len() {
            return Err(io::Error::other(format!(
                "Query is too complex to parse (limit {} AST JSON bytes).",
                self.limit
            )));
        }
        let new_len = self.bytes.len() + bytes.len();
        if new_len > self.bytes.capacity() {
            let capacity = self
                .bytes
                .capacity()
                .saturating_mul(2)
                .max(new_len)
                .min(self.limit);
            self.bytes.reserve_exact(capacity - self.bytes.len());
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn check_json_depth(json: &[u8]) -> Result<(), String> {
    let mut depth = 0;
    let mut in_string = false;
    let mut escaped = false;
    for &byte in json {
        if in_string {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_string = false;
            }
        } else {
            match byte {
                b'"' => in_string = true,
                b'[' | b'{' => {
                    depth += 1;
                    if depth >= JSON_RECURSION_BUDGET {
                        return Err(format!(
                            "Query is too complex to parse (JSON recursion budget {JSON_RECURSION_BUDGET} levels)."
                        ));
                    }
                }
                b']' | b'}' => depth -= 1,
                _ => {}
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::ClickHouseDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn token_budget_boundaries() {
        let at_limit = format!("SELECT {}", vec!["1"; 10_000].join(","));
        let below_limit = format!("SELECT {};", vec!["1"; 9_999].join(","));
        for query in [below_limit, at_limit.clone()] {
            assert!(tokenize(&ClickHouseDialect {}, &query).is_ok());
        }
        let error = tokenize(&ClickHouseDialect {}, &(at_limit + ";")).unwrap_err();
        assert!(error.to_string().contains("20000 significant tokens"));
    }

    #[test]
    fn comments_and_literal_contents_are_not_significant_tokens() {
        let query = format!(
            "SELECT '{}' /* {} */ -- {}\n",
            "[{}] UNION ALL ".repeat(20_000),
            "+ ".repeat(20_000),
            "SELECT ".repeat(20_000)
        );
        assert!(tokenize(&ClickHouseDialect {}, &query).is_ok());
    }

    #[test]
    fn writer_accepts_limit_and_refuses_excess_without_growing() {
        let mut writer = LimitedWriter::new(16);
        writer.write_all(&[b'x'; 15]).unwrap();
        writer.write_all(b"x").unwrap();
        assert_eq!(writer.bytes.len(), 16);
        assert!(writer.write_all(b"x").is_err());
        assert_eq!(writer.bytes.len(), 16);
        assert!(writer.bytes.capacity() <= 16);

        let mut writer = LimitedWriter::new(16);
        assert!(writer.write_all(&[b'x'; 17]).is_err());
        assert!(writer.bytes.is_empty());
    }

    #[test]
    fn writer_counts_escaped_json_bytes() {
        let mut writer = LimitedWriter::new(4);
        serde_json::to_writer(&mut writer, "\n").unwrap();
        assert_eq!(writer.bytes, br#""\n""#);
        assert!(serde_json::to_writer(LimitedWriter::new(3), "\n").is_err());
    }

    #[test]
    fn encoding_refuses_oversized_json() {
        let query = format!("SELECT '{}'", "\0".repeat(MAX_AST_JSON_BYTES / 6));
        let ast = Parser::parse_sql(&ClickHouseDialect {}, &query).unwrap();
        let error = encode(&ast).unwrap_err();
        assert!(error.contains("16777216 AST JSON bytes"), "{error}");
    }

    #[test]
    fn depth_budget_matches_serde_at_boundary() {
        for depth in [126, 127, 128, 129] {
            let json = format!("{}0{}", "[".repeat(depth), "]".repeat(depth));
            let accepted = check_json_depth(json.as_bytes()).is_ok();
            let deserialized = serde_json::from_str::<serde_json::Value>(&json).is_ok();
            assert_eq!(accepted, depth < JSON_RECURSION_BUDGET);
            assert_eq!(accepted, deserialized, "depth {depth}");
        }
    }

    #[test]
    fn depth_scan_ignores_escaped_quotes_backslashes_and_containers_in_strings() {
        let value = format!("{}\\\"{}\\", "[{".repeat(200), "}]".repeat(200));
        let json = serde_json::to_vec(&vec![value]).unwrap();
        assert!(check_json_depth(&json).is_ok());
    }
}
