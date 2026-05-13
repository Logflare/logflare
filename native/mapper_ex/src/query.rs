use rustler::types::ListIterator;
use rustler::{Binary, Encoder, Env, Term};

use crate::path::PathSegment;
use crate::string_filters::{self, StringFilters};

/// Evaluates a path against an Elixir term (map/list).
///
/// Uses iterative traversal for key-only and index segments (the common case),
/// falling back to a separate function for wildcard fan-out.
/// Returns the matched value or the `nil` atom for missing paths.
///
/// When `flat_keys` is true, all Key segments are joined with `.` and resolved
/// as a single literal key lookup against the root map. This supports
/// pre-flattened input where `"resource.service.name"` is a top-level key.
pub fn evaluate<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    nil: Term<'a>,
    flat_keys: bool,
) -> Term<'a> {
    if segments.is_empty() {
        return term;
    }

    if flat_keys {
        return evaluate_flat(env, term, segments, nil);
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

/// Flat-key evaluation: joins all Key segments with `.` and does a single
/// `map_get` on the root term. Non-Key segments (Wildcard, Index) cause
/// a nil return since they have no flat-key representation.
fn evaluate_flat<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    nil: Term<'a>,
) -> Term<'a> {
    let mut parts: Vec<&str> = Vec::with_capacity(segments.len());

    for seg in segments {
        match seg {
            PathSegment::Key(k) => parts.push(k.as_str()),
            // Wildcard/Index paths don't have a flat-key equivalent
            _ => return nil,
        }
    }

    let flat_key = parts.join(".");
    let key_term = flat_key.encode(env);

    match term.map_get(key_term) {
        Ok(value) => value,
        Err(_) => nil,
    }
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
    // Wildcard fan-out always uses nested evaluation (flat_keys=false)
    let results: Vec<Term<'a>> = iter
        .map(|item| evaluate(env, item, &segments[1..], nil, false))
        .collect();
    results.encode(env)
}

/// Evaluates multiple paths against a document, returning the first
/// non-nil result. For String field types, also skips empty strings.
/// When filters are provided, resolved strings must pass all filters.
pub fn evaluate_first<'a>(
    env: Env<'a>,
    document: Term<'a>,
    paths: &[Vec<PathSegment>],
    skip_empty_strings: bool,
    string_filters: Option<&StringFilters>,
    nil: Term<'a>,
    flat_keys: bool,
) -> Term<'a> {
    for segments in paths {
        let result = evaluate(env, document, segments, nil, flat_keys);
        if result == nil {
            continue;
        }
        if skip_empty_strings {
            // Check binary length without allocating a String
            if let Ok(b) = result.decode::<Binary>() {
                if b.is_empty() {
                    continue;
                }
                // Piggyback filter check on already-decoded binary
                if let Some(f) = string_filters {
                    if !string_filters::passes_filters(b.as_slice(), f) {
                        continue;
                    }
                }
            }
        }
        return result;
    }

    nil
}
