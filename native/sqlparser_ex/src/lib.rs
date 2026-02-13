use rustler::Atom;
use rustler::NifResult;
use rustler::NifTuple;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use sqlparser::parser::ParserError::ParserError;

mod atoms {
    rustler::atoms! {
      ok,
      error,
    }
}

#[derive(NifTuple)]
struct Response {
    status: Atom,
    message: String,
}

#[rustler::nif]
fn parse(dialect_str: &str, query: &str) -> NifResult<Response> {
    let result = match dialect_str {
        "bigquery" => Parser::parse_sql(&BigQueryDialect {}, query),
        "clickhouse" => Parser::parse_sql(&ClickHouseDialect {}, query),
        "postgres" => Parser::parse_sql(&PostgreSqlDialect {}, query),
        _ => Err(ParserError(
            "Parser for this dialect is not supported.".to_string(),
        )),
    };
    match result {
        Ok(v) => Ok(Response {
            status: atoms::ok(),
            message: serde_json::to_string(&v).unwrap(),
        }),
        Err(v) => Ok(Response {
            status: atoms::error(),
            message: v.to_string(),
        }),
    }
}

#[rustler::nif]
fn to_string(json: &str) -> NifResult<Response> {
    let nodes: Vec<sqlparser::ast::Statement> = serde_json::from_str(json).unwrap();

    let mut parts = vec![];
    for node in nodes {
        parts.push(format!("{}", node))
    }

    Ok(Response {
        status: atoms::ok(),
        message: parts.join("\n"),
    })
}

rustler::init!("Elixir.Logflare.Sql.Parser.Native");
