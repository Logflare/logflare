use rustler::Atom;
use rustler::NifResult;
use rustler::NifTuple;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

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

impl Response {
    fn ok(message: String) -> Self {
        Self {
            status: atoms::ok(),
            message,
        }
    }

    fn error(message: &str) -> Self {
        Self {
            status: atoms::error(),
            message: message.to_string(),
        }
    }
}

#[rustler::nif]
fn parse(dialect_str: &str, query: &str) -> NifResult<Response> {
    let result = match dialect_str {
        "bigquery" => Parser::parse_sql(&BigQueryDialect {}, query),
        "clickhouse" => Parser::parse_sql(&ClickHouseDialect {}, query),
        "postgres" => Parser::parse_sql(&PostgreSqlDialect {}, query),
        _ => return Ok(Response::error("Parser for this dialect is not supported.")),
    };

    match result {
        Ok(ast) => match serde_json::to_string(&ast) {
            Ok(json) => Ok(Response::ok(json)),
            Err(e) => Ok(Response::error(&format!("JSON serialization error: {}", e))),
        },
        Err(e) => Ok(Response::error(&e.to_string())),
    }
}

#[rustler::nif]
fn to_string(json: &str) -> NifResult<Response> {
    let value: serde_json::Value = match serde_json::from_str(json) {
        Ok(val) => val,
        Err(e) => return Ok(Response::error(&format!("JSON parsing error: {}", e))),
    };

    let fixed_json = match serde_json::to_string(&value) {
        Ok(json) => json,
        Err(_) => json.to_string(),
    };

    let statements: Vec<sqlparser::ast::Statement> = match serde_json::from_str(&fixed_json) {
        Ok(nodes) => nodes,
        Err(e) => return Ok(Response::error(&format!("JSON deserialization error: {}", e))),
    };

    let sql_parts: Vec<String> = statements.iter().map(|stmt| stmt.to_string()).collect();
    Ok(Response::ok(sql_parts.join("\n")))
}

rustler::init!("Elixir.Logflare.Sql.Parser.Native");
