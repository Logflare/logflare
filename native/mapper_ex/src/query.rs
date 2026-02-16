use rustler::types::ListIterator;
use rustler::{Binary, Encoder, Env, Term};

use crate::path::PathSegment;

/// Evaluates a path against an Elixir term (map/list).
///
/// Uses iterative traversal for key-only and index segments (the common case),
/// falling back to a separate function for wildcard fan-out.
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

    let mut current = term;
    let mut i = 0;

    while i < segments.len() {
        match &segments[i] {
            PathSegment::Key(key) => {
                let key_term = key.encode(env);
                match current.map_get(key_term) {
                    Ok(value) => current = value,
                    Err(_) => return nil,
                }
                i += 1;
            }
            PathSegment::Wildcard => {
                // Fall back to wildcard fan-out for the remaining segments
                return evaluate_wildcard(env, current, &segments[i..], nil);
            }
            PathSegment::Index(idx) => match current.decode::<ListIterator>() {
                Ok(mut iter) => match iter.nth(*idx) {
                    Some(item) => {
                        current = item;
                        i += 1;
                    }
                    None => return nil,
                },
                Err(_) => return nil,
            },
        }
    }

    current
}

/// Handle wildcard fan-out: segments[0] is Wildcard.
fn evaluate_wildcard<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    nil: Term<'a>,
) -> Term<'a> {
    let iter = match term.decode::<ListIterator>() {
        Ok(iter) => iter,
        Err(_) => return nil,
    };
    let results: Vec<Term<'a>> = iter
        .map(|item| evaluate(env, item, &segments[1..], nil))
        .collect();
    results.encode(env)
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
