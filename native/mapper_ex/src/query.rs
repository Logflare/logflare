use rustler::{Encoder, Env, Term};

use crate::path::PathSegment;

mod atoms {
    rustler::atoms! {
        nil,
    }
}

/// Evaluates a path against an Elixir term (map/list).
///
/// Traverses the term using BEAM-native map_get for key lookups,
/// list iteration for wildcards, and list indexing for index access.
/// Returns the matched value or the `nil` atom for missing paths.
pub fn evaluate<'a>(env: Env<'a>, term: Term<'a>, segments: &[PathSegment]) -> Term<'a> {
    if segments.is_empty() {
        return term;
    }

    match &segments[0] {
        PathSegment::Key(key) => {
            let key_term = key.encode(env);
            match term.map_get(key_term) {
                Ok(value) => evaluate(env, value, &segments[1..]),
                Err(_) => atoms::nil().encode(env),
            }
        }
        PathSegment::Wildcard => {
            let items: Vec<Term<'a>> = match term.decode::<Vec<Term<'a>>>() {
                Ok(list) => list,
                Err(_) => return atoms::nil().encode(env),
            };

            let results: Vec<Term<'a>> = items
                .into_iter()
                .map(|item| evaluate(env, item, &segments[1..]))
                .collect();

            results.encode(env)
        }
        PathSegment::Index(idx) => {
            let items: Vec<Term<'a>> = match term.decode::<Vec<Term<'a>>>() {
                Ok(list) => list,
                Err(_) => return atoms::nil().encode(env),
            };

            match items.get(*idx) {
                Some(item) => evaluate(env, *item, &segments[1..]),
                None => atoms::nil().encode(env),
            }
        }
    }
}

/// Evaluates multiple paths against a document, returning the first
/// non-nil result. For String field types, also skips empty strings.
pub fn evaluate_first<'a>(
    env: Env<'a>,
    document: Term<'a>,
    paths: &[Vec<PathSegment>],
    skip_empty_strings: bool,
) -> Term<'a> {
    let nil = atoms::nil().encode(env);

    for segments in paths {
        let result = evaluate(env, document, segments);
        if result == nil {
            continue;
        }
        if skip_empty_strings {
            if let Ok(s) = result.decode::<String>() {
                if s.is_empty() {
                    continue;
                }
            }
        }
        return result;
    }

    nil
}
