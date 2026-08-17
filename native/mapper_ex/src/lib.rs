mod coerce;
mod mapper;
mod mapping;
mod path;
mod query;
mod string_filters;

use rustler::{Encoder, Env, NewBinary, NifResult, Resource, ResourceArc, Term};

use mapping::CompiledMapping;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

#[inline]
fn encode_string<'a>(env: Env<'a>, value: &str) -> Term<'a> {
    let mut binary = NewBinary::new(env, value.len());
    binary.as_mut_slice().copy_from_slice(value.as_bytes());
    binary.into()
}

#[inline]
fn encode_integer<'a>(env: Env<'a>, value: i64) -> Term<'a> {
    let mut buffer = itoa::Buffer::new();
    encode_string(env, buffer.format(value))
}

/// A compiled full mapping configuration stored as a Rustler Resource.
///
/// Created once via compile_mapping/1 and reused for every map/2 call.
pub struct CompiledMappingResource {
    pub mapping: CompiledMapping,
}

impl Resource for CompiledMappingResource {}

/// Compiles a mapping configuration map into a NIF resource.
///
/// Returns `{:ok, resource}` if valid, or `{:error, reason}` if invalid.
#[rustler::nif]
fn compile_mapping<'a>(env: Env<'a>, config: Term<'a>) -> NifResult<Term<'a>> {
    match mapping::decode_mapping(env, config) {
        Ok(compiled) => {
            let resource = ResourceArc::new(CompiledMappingResource { mapping: compiled });
            Ok((atoms::ok(), resource).encode(env))
        }
        Err(reason) => Ok((atoms::error(), reason).encode(env)),
    }
}

/// Maps a single document using a pre-compiled mapping.
///
/// Returns the mapped Elixir map directly (no ok/error tuple).
///
/// When `flat_keys` is `true`, dotted paths like `$.resource.service.name`
/// are resolved as literal key lookups on the input map instead of nested
/// map navigation. This allows mapping against pre-flattened input.
#[rustler::nif]
fn map<'a>(
    env: Env<'a>,
    body: Term<'a>,
    compiled: ResourceArc<CompiledMappingResource>,
    flat_keys: bool,
) -> Term<'a> {
    mapper::map_single(env, body, &compiled.mapping, flat_keys)
}

fn on_load(env: Env, _info: Term) -> bool {
    env.register::<CompiledMappingResource>().is_ok()
}

rustler::init!("Elixir.Logflare.Mapper.Native", load = on_load);
