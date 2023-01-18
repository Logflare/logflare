use rustler::NifTuple;
use rustler::NifResult;
use rustler::Atom;
use sqlparser::dialect::BigQueryDialect;
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

#[rustler::nif]
fn parse(query: &str) -> NifResult<Response> {
    let dialect = BigQueryDialect {}; // or AnsiDialect, or your own dialect ...
    let result = Parser::parse_sql(&dialect, query);
    match result {
        Ok(v) => Ok(Response{status: atoms::ok(), message: serde_json::to_string(&v).unwrap()}),
        Err(v) => Ok(Response{status: atoms::error(), message: v.to_string()}),
    }
}


#[rustler::nif]
fn to_string(json: &str) -> NifResult<Response> {
    let nodes: Vec<sqlparser::ast::Statement> = serde_json::from_str(json).unwrap();
    
    let mut parts = vec![];
    for node in nodes {
        parts.push(
            format!("{}", node)
        )
    }

    return Ok(Response{status: atoms::ok(), message: parts.join("\n")});
}

rustler::init!("Elixir.Logflare.SqlV2.Parser.Native", [parse, to_string]);
