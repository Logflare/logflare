mod coerce;
mod mapper;
mod mapping;
mod path;
mod query;

use rustler::{Encoder, Env, NifResult, Resource, ResourceArc, Term};

use mapping::CompiledMapping;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
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
#[rustler::nif]
fn map<'a>(
    env: Env<'a>,
    body: Term<'a>,
    compiled: ResourceArc<CompiledMappingResource>,
) -> Term<'a> {
    mapper::map_single(env, body, &compiled.mapping)
}

/// Maps a batch of documents using a pre-compiled mapping.
///
/// Returns a list of mapped Elixir maps.
#[rustler::nif(schedule = "DirtyCpu")]
fn map_batch<'a>(
    env: Env<'a>,
    bodies: Vec<Term<'a>>,
    compiled: ResourceArc<CompiledMappingResource>,
) -> Vec<Term<'a>> {
    bodies
        .into_iter()
        .map(|body| mapper::map_single(env, body, &compiled.mapping))
        .collect()
}

fn on_load(env: Env, _info: Term) -> bool {
    env.register::<CompiledMappingResource>().is_ok()
}

rustler::init!("Elixir.Logflare.Mapper.Native", load = on_load);
