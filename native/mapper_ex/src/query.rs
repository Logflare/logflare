use std::collections::HashMap;

use rustler::types::map::MapIterator;
use rustler::types::ListIterator;
use rustler::{Binary, Encoder, Env, Term};

use crate::path::{CompiledPath, PathSegment};
use crate::string_filters::{self, StringFilters};

pub struct QueryCache<'a> {
    values: Vec<Option<Term<'a>>>,
    root_size: usize,
    root_preloaded: bool,
    nil: Term<'a>,
}

impl<'a> QueryCache<'a> {
    pub fn new(size: usize, root_size: usize, nil: Term<'a>) -> Self {
        Self {
            values: vec![None; size],
            root_size,
            root_preloaded: false,
            nil,
        }
    }

    pub fn preload_root(&mut self, document: Term<'a>, keys: &HashMap<Vec<u8>, usize>) {
        if keys.is_empty() {
            return;
        }

        self.root_preloaded = true;
        if let Some(entries) = MapIterator::new(document) {
            for (key, value) in entries {
                if let Ok(binary) = key.decode::<Binary>() {
                    if let Some(index) = keys.get(binary.as_slice()) {
                        self.values[*index] = Some(value);
                    }
                }
            }
        }
    }

    #[inline]
    fn get(&self, index: Option<usize>) -> Option<Term<'a>> {
        index.and_then(|index| {
            self.values[index]
                .or_else(|| (self.root_preloaded && index < self.root_size).then_some(self.nil))
        })
    }

    #[inline]
    fn put(&mut self, index: Option<usize>, value: Term<'a>) {
        if let Some(index) = index {
            self.values[index] = Some(value);
        }
    }
}

/// Evaluates a path against an Elixir term (map/list).
///
/// Uses iterative traversal for key-only and index segments (the common case),
/// falling back to a separate function for wildcard fan-out. Repeated path
/// prefixes are served from the per-document cache populated during traversal.
/// Returns the matched value or the `nil` atom for missing paths.
///
/// When `flat_keys` is true, the precompiled dot-joined key is resolved as a
/// single literal key lookup against the root map.
#[inline]
pub fn evaluate<'a>(
    env: Env<'a>,
    term: Term<'a>,
    path: &CompiledPath,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut QueryCache<'a>,
) -> Term<'a> {
    if path.segments.is_empty() {
        return term;
    }

    if flat_keys {
        return evaluate_flat(env, term, path, nil, cache);
    }
    if !path.cached {
        return evaluate_uncached(env, term, &path.segments, nil);
    }

    evaluate_nested(env, term, &path.segments, &path.cache_indices, nil, cache)
}

pub fn evaluate_wildcard_mapped<'a, F>(
    env: Env<'a>,
    term: Term<'a>,
    path: &CompiledPath,
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut QueryCache<'a>,
    mut mapper: F,
) -> Option<Term<'a>>
where
    F: FnMut(Term<'a>) -> Option<Term<'a>>,
{
    if flat_keys {
        return None;
    }
    let wildcard_index = path.wildcard_index?;
    let current = if wildcard_index == 0 {
        term
    } else if path.cached {
        evaluate_nested(
            env,
            term,
            &path.segments[..wildcard_index],
            &path.cache_indices[..wildcard_index],
            nil,
            cache,
        )
    } else {
        evaluate_uncached(env, term, &path.segments[..wildcard_index], nil)
    };

    let iter = match current.decode::<ListIterator>() {
        Ok(iter) => iter,
        Err(_) => return Some(Vec::<Term>::new().encode(env)),
    };
    let remaining = &path.segments[wildcard_index + 1..];
    let mut results = Vec::new();
    for item in iter {
        let value = evaluate_uncached(env, item, remaining, nil);
        if let Some(value) = mapper(value) {
            results.push(value);
        }
    }
    Some(results.encode(env))
}

fn evaluate_uncached<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    nil: Term<'a>,
) -> Term<'a> {
    let mut current = term;
    let mut i = 0;

    while i < segments.len() {
        match &segments[i] {
            PathSegment::Key(key) => match current.map_get(crate::encode_string(env, key)) {
                Ok(value) => current = value,
                Err(_) => return nil,
            },
            PathSegment::Index(index) => match current.decode::<ListIterator>() {
                Ok(mut iter) => match iter.nth(*index) {
                    Some(value) => current = value,
                    None => return nil,
                },
                Err(_) => return nil,
            },
            PathSegment::Wildcard => {
                let iter = match current.decode::<ListIterator>() {
                    Ok(iter) => iter,
                    Err(_) => return nil,
                };
                let values: Vec<Term<'a>> = iter
                    .map(|item| evaluate_uncached(env, item, &segments[i + 1..], nil))
                    .collect();
                return values.encode(env);
            }
        }
        i += 1;
    }

    current
}

#[inline]
fn evaluate_nested<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    cache_indices: &[Option<usize>],
    nil: Term<'a>,
    cache: &mut QueryCache<'a>,
) -> Term<'a> {
    let mut current = term;
    let mut i = 0;

    while i < segments.len() {
        let cache_index = cache_indices[i];
        if let Some(value) = cache.get(cache_index) {
            if value == nil {
                return nil;
            }
            current = value;
            i += 1;
            continue;
        }

        let value = match &segments[i] {
            PathSegment::Key(key) => current
                .map_get(crate::encode_string(env, key))
                .unwrap_or(nil),
            PathSegment::Wildcard => {
                return evaluate_wildcard(
                    env,
                    current,
                    &segments[i + 1..],
                    &cache_indices[i + 1..],
                    nil,
                    cache,
                );
            }
            PathSegment::Index(index) => match current.decode::<ListIterator>() {
                Ok(mut iter) => iter.nth(*index).unwrap_or(nil),
                Err(_) => nil,
            },
        };

        cache.put(cache_index, value);
        if value == nil {
            return nil;
        }
        current = value;
        i += 1;
    }

    current
}

#[inline]
fn evaluate_flat<'a>(
    env: Env<'a>,
    term: Term<'a>,
    path: &CompiledPath,
    nil: Term<'a>,
    cache: &mut QueryCache<'a>,
) -> Term<'a> {
    let key = match &path.flat_key {
        Some(key) => key,
        None => return nil,
    };

    if let Some(value) = cache.get(path.flat_cache_index) {
        return value;
    }

    let value = term.map_get(crate::encode_string(env, key)).unwrap_or(nil);
    cache.put(path.flat_cache_index, value);
    value
}

fn evaluate_wildcard<'a>(
    env: Env<'a>,
    term: Term<'a>,
    segments: &[PathSegment],
    cache_indices: &[Option<usize>],
    nil: Term<'a>,
    cache: &mut QueryCache<'a>,
) -> Term<'a> {
    let iter = match term.decode::<ListIterator>() {
        Ok(iter) => iter,
        Err(_) => return nil,
    };
    let results: Vec<Term<'a>> = iter
        .map(|item| evaluate_nested(env, item, segments, cache_indices, nil, cache))
        .collect();
    results.encode(env)
}

/// Evaluates multiple paths against a document, returning the first
/// non-nil result. For String field types, also skips empty strings.
/// When filters are provided, resolved strings must pass all filters.
#[inline]
pub fn evaluate_first<'a>(
    env: Env<'a>,
    document: Term<'a>,
    paths: &[CompiledPath],
    string_options: (bool, Option<&StringFilters>),
    nil: Term<'a>,
    flat_keys: bool,
    cache: &mut QueryCache<'a>,
) -> Term<'a> {
    let (skip_empty_strings, filters) = string_options;
    for path in paths {
        let result = evaluate(env, document, path, nil, flat_keys, cache);
        if result == nil {
            continue;
        }
        if skip_empty_strings {
            if let Ok(binary) = result.decode::<Binary>() {
                if binary.is_empty() {
                    continue;
                }
                if let Some(filters) = filters {
                    if !string_filters::passes_filters(binary.as_slice(), filters) {
                        continue;
                    }
                }
            }
        }
        return result;
    }

    nil
}
