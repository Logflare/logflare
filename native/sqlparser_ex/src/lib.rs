use rustler::Atom;
use rustler::NifResult;
use rustler::NifTuple;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::dialect::Dialect;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::{Parser, ParserError};

mod clickhouse;
mod limits;

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

/// Stack size for the worker thread that runs each parse/format call.
///
/// sqlparser builds chains of binary operators (`a OR b OR c ...`) in a loop, so its recursion
/// limit does not bound the depth of the AST. Serializing, formatting and dropping that AST all
/// recurse once per level. On a BEAM scheduler thread (about 1 MiB of stack by default, 320 KiB on
/// a dirty CPU scheduler) a deep enough query overflows the stack, and a stack overflow inside a
/// NIF kills the whole VM. The worker thread gets a large stack instead; the memory is only
/// reserved, and pages are committed as they are used. Byte/token/output/depth budgets bound
/// parser resource use separately; even small UNION queries need the larger worker stack.
const WORKER_STACK_SIZE: usize = 512 * 1024 * 1024;

type WorkResult = Result<String, String>;

/// Runs `work` on a dedicated thread with `WORKER_STACK_SIZE` of stack and waits for it.
/// A panic in `work` becomes an error response instead of unwinding into the VM.
fn run_on_worker<F>(work: F) -> NifResult<Response>
where
    F: FnOnce() -> WorkResult + Send + 'static,
{
    let result = std::thread::Builder::new()
        .name("sqlparser_ex".to_string())
        .stack_size(WORKER_STACK_SIZE)
        .spawn(work)
        .map_err(|e| format!("could not start sqlparser worker: {e}"))
        .and_then(|handle| {
            handle
                .join()
                .unwrap_or_else(|_| Err("sqlparser worker panicked".to_string()))
        });

    Ok(match result {
        Ok(message) => Response {
            status: atoms::ok(),
            message,
        },
        Err(message) => Response {
            status: atoms::error(),
            message,
        },
    })
}

#[rustler::nif(schedule = "DirtyCpu")]
fn parse(dialect_str: &str, query: &str) -> NifResult<Response> {
    let dialect = dialect_str.to_owned();
    let query = query.to_owned();
    run_on_worker(move || parse_with_dialect(&dialect, &query))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn parse_clickhouse_converted(query: &str) -> NifResult<Response> {
    let query = query.to_owned();
    run_on_worker(move || encode_parse_result(clickhouse::parse(&query)))
}

#[rustler::nif(schedule = "DirtyCpu")]
fn to_string(json: &str) -> NifResult<Response> {
    let json = json.to_owned();
    run_on_worker(move || format_statements(&json))
}

fn parse_with_dialect(dialect: &str, query: &str) -> WorkResult {
    let result = match dialect {
        "bigquery" => parse_sql(&BigQueryDialect {}, query),
        "clickhouse" => parse_sql(&ClickHouseDialect {}, query),
        "postgres" => parse_sql(&PostgreSqlDialect {}, query),
        _ => Err(ParserError::ParserError(
            "Parser for this dialect is not supported.".to_string(),
        )),
    };
    encode_parse_result(result)
}

fn parse_sql(
    dialect: &dyn Dialect,
    query: &str,
) -> Result<Vec<sqlparser::ast::Statement>, ParserError> {
    let tokens = limits::tokenize(dialect, query)?;
    Parser::new(dialect)
        .with_tokens_with_locations(tokens)
        .parse_statements()
}

fn encode_parse_result(result: Result<Vec<sqlparser::ast::Statement>, ParserError>) -> WorkResult {
    let statements = result.map_err(|e| e.to_string())?;
    limits::encode(&statements)
}

fn format_statements(json: &str) -> WorkResult {
    let nodes: Vec<sqlparser::ast::Statement> =
        serde_json::from_str(json).map_err(|e| e.to_string())?;

    let parts: Vec<String> = nodes.iter().map(|node| format!("{}", node)).collect();
    Ok(parts.join("\n"))
}

rustler::init!("Elixir.Logflare.Sql.Parser.Native");

#[cfg(test)]
mod tests {
    use super::*;

    fn or_chain(terms: usize) -> String {
        let mut query = String::from("select a from t where a = 0");
        for i in 1..terms {
            query.push_str(&format!(" or a = {i}"));
        }
        query
    }

    fn on_worker_stack(work: impl FnOnce() -> WorkResult + Send + 'static) -> WorkResult {
        std::thread::Builder::new()
            .stack_size(WORKER_STACK_SIZE)
            .spawn(work)
            .unwrap()
            .join()
            .unwrap()
    }

    #[test]
    fn deep_or_chain_is_refused_on_worker_stack() {
        let query = or_chain(50_000);
        let error = on_worker_stack(move || parse_with_dialect("clickhouse", &query)).unwrap_err();
        assert!(error.contains("20000 significant tokens"), "{error}");
    }

    #[test]
    fn deep_chain_at_token_limit_is_refused_by_depth_budget() {
        on_worker_stack(|| {
            let query = format!("SELECT {}", vec!["1"; 10_000].join("+"));
            for result in [
                parse_with_dialect("clickhouse", &query),
                encode_parse_result(clickhouse::parse(&query)),
            ] {
                let error = result.unwrap_err();
                assert!(error.contains("JSON recursion budget 128"), "{error}");
            }
            Ok(String::new())
        })
        .unwrap();
    }

    #[test]
    fn parse_depth_matches_formatter_for_each_dialect() {
        on_worker_stack(|| {
            for dialect in ["bigquery", "clickhouse", "postgres"] {
                for (terms, nesting) in (50..=65).flat_map(|terms| (0..=3).map(move |n| (terms, n)))
                {
                    let query = format!(
                        "SELECT {}{}{}",
                        "(".repeat(nesting),
                        vec!["1"; terms].join("+"),
                        ")".repeat(nesting)
                    );
                    let ast = match dialect {
                        "bigquery" => Parser::parse_sql(&BigQueryDialect {}, &query),
                        "postgres" => Parser::parse_sql(&PostgreSqlDialect {}, &query),
                        _ => Parser::parse_sql(&ClickHouseDialect {}, &query),
                    }
                    .unwrap();
                    let json = serde_json::to_string(&ast).unwrap();
                    let accepted = format_statements(&json).is_ok();
                    assert_eq!(parse_with_dialect(dialect, &query).is_ok(), accepted);
                    assert_eq!(
                        encode_parse_result(clickhouse::parse(&query)).is_ok(),
                        accepted
                    );
                }
            }
            Ok(String::new())
        })
        .unwrap();
    }

    #[test]
    fn unsupported_dialect_is_an_error() {
        let error = parse_with_dialect("mysql", "select 1").unwrap_err();
        assert!(error.contains("not supported"), "{error}");
    }
}
