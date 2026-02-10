use rustler::types::ListIterator;
use rustler::{Binary, Encoder, Env, Term};

use crate::path::PathSegment;

/// Evaluates a path against an Elixir term (map/list).
///
/// Traverses the term using BEAM-native map_get for key lookups,
/// list iteration for wildcards, and list indexing for index access.
/// Returns the matched value or the `nil` atom for missing paths.
pub fn evaluate<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    nil: Term<'a>,
) -> Term<'a> {
    if segments.is_empty() {
        return term;
    }

    match &segments[0] {
        PathSegment::Key(key) => {
            let key_term = key.encode(env);
            match term.map_get(key_term) {
                Ok(value) => evaluate(env, value, &segments[1..], nil),
                Err(_) => nil,
            }
        }
        PathSegment::Wildcard => {
            let items: Vec<Term<'a>> = match term.decode::<Vec<Term<'a>>>() {
                Ok(list) => list,
                Err(_) => return nil,
            };

            let results: Vec<Term<'a>> = items
                .into_iter()
                .map(|item| evaluate(env, item, &segments[1..], nil))
                .collect();

            results.encode(env)
        }
        PathSegment::Index(idx) => match term.decode::<ListIterator>() {
            Ok(mut iter) => match iter.nth(*idx) {
                Some(item) => evaluate(env, item, &segments[1..], nil),
                None => nil,
            },
            Err(_) => nil,
        },
    }
}

/// Evaluates multiple paths against a document, returning the first
/// non-nil result. For String field types, also skips empty strings.
pub fn evaluate_first<'a>(
    env: Env<'a>,
    document: Term<'a>,
    paths: &[Vec<PathSegment>],
    skip_empty_strings: bool,
    nil: Term<'a>,
) -> Term<'a> {
    for segments in paths {
        let result = evaluate(env, document, segments, nil);
        if result == nil {
            continue;
        }
        if skip_empty_strings {
            // Check binary length without allocating a String
            if let Ok(b) = result.decode::<Binary>() {
                if b.is_empty() {
                    continue;
                }
            }
        }
        return result;
    }

    nil
}
