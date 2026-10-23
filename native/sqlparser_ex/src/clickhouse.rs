use sqlparser::ast::Statement;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::keywords::Keyword;
use sqlparser::parser::{Parser, ParserError};
use sqlparser::tokenizer::{Token, Tokenizer, Word};

pub fn parse(query: &str) -> Result<Vec<Statement>, ParserError> {
    let dialect = ClickHouseDialect {};
    let mut tokens = Tokenizer::new(&dialect, query).tokenize_with_location()?;
    let significant: Vec<usize> = tokens
        .iter()
        .enumerate()
        .filter_map(|(index, token)| {
            (!matches!(token.token, Token::Whitespace(_))).then_some(index)
        })
        .collect();

    for window in significant.windows(3) {
        if matches!(tokens[window[0]].token, Token::LBrace)
            && matches!(tokens[window[2]].token, Token::Colon)
        {
            if let Token::Placeholder(name) = &tokens[window[1]].token {
                if name.strip_prefix('$').is_some_and(|index| {
                    !index.is_empty() && index.bytes().all(|byte| byte.is_ascii_digit())
                }) {
                    tokens[window[1]].token = Token::Word(Word {
                        value: name.clone(),
                        quote_style: None,
                        keyword: Keyword::NoKeyword,
                    });
                }
            }
        }
    }

    Parser::new(&dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

#[cfg(test)]
mod tests {
    use super::parse;

    #[test]
    fn positional_parameters_round_trip_with_types_and_repeated_names() {
        let sql = "SELECT {$0:String}, {$1:Array(UInt64)}, {$0:String} FROM t \
                   WHERE a = {$2:DateTime64(6, 'UTC')}";
        let ast = parse(sql).unwrap();
        let formatted = ast[0].to_string();
        assert!(formatted.contains("{$0: String}"));
        assert!(formatted.contains("{$1: Array(UInt64)}"));
        assert!(formatted.contains("{$2: DateTime64(6, 'UTC')}"));
        assert_eq!(parse(&formatted).unwrap(), ast);
    }

    #[test]
    fn literals_comments_and_quoted_identifiers_are_not_parameter_tokens() {
        let sql = "SELECT '{$0:String}' AS literal, 1 AS \"$0\", \
                   { /* before */ $0 /* after */ : String } -- {$1:UInt64}";
        let formatted = parse(sql).unwrap()[0].to_string();
        assert!(formatted.contains("'{$0:String}' AS literal"));
        assert!(formatted.contains("AS \"$0\""));
        assert!(formatted.contains("{$0: String}"));
        assert!(!formatted.contains("$1"));
    }

    #[test]
    fn standalone_placeholders_and_named_parameters_keep_their_existing_syntax() {
        let formatted = parse("SELECT $0, {value:String}").unwrap()[0].to_string();
        assert_eq!(formatted, "SELECT $0, {value: String}");
    }

    #[test]
    fn customer_parser_stays_strict_while_converted_sql_accepts_native_bindings() {
        for sql in ["SELECT {$0:String}", "SELECT a FROM {$0:Identifier}"] {
            assert!(sqlparser::parser::Parser::parse_sql(
                &sqlparser::dialect::ClickHouseDialect {},
                sql
            )
            .is_err());
        }
        assert!(parse("SELECT {$0:String}").is_ok());
        assert!(parse("SELECT a FROM {$0:Identifier}").is_err());
    }

    #[test]
    fn malformed_positional_parameters_are_rejected() {
        for sql in ["SELECT {$0:}", "SELECT {$0:String", "SELECT {$0 String}"] {
            assert!(parse(sql).is_err(), "unexpectedly accepted {sql}");
        }
    }
}
