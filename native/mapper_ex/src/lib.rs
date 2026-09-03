mod clickhouse_rowbinary;
mod coerce;
mod derive;
mod mapper;
mod mapping;
mod ndjson;
mod path;
mod query;
mod string_filters;

use rustler::{Binary, Encoder, Env, NewBinary, NifResult, Resource, ResourceArc, Term};

use mapping::{CompiledMapping, CompiledOutput};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        ch_row_binary,
        ndjson,
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

/// Maps a single document using a pre-compiled mapping and its configured output.
#[rustler::nif]
fn map<'a>(
    env: Env<'a>,
    document: Term<'a>,
    compiled: ResourceArc<CompiledMappingResource>,
    options: Term<'a>,
) -> Term<'a> {
    match &compiled.mapping.output {
        CompiledOutput::Map => match decode_flat_keys(options) {
            Ok(flat_keys) => mapper::map_single(env, document, &compiled.mapping, flat_keys),
            Err(reason) => (atoms::error(), reason).encode(env),
        },
        CompiledOutput::ClickHouseRowBinary(layout) => {
            match map_clickhouse_output(env, document, &compiled.mapping, layout, options) {
                Ok(binary) => (atoms::ok(), binary.release(env)).encode(env),
                Err(reason) => (atoms::error(), reason).encode(env),
            }
        }
        CompiledOutput::Ndjson(layout) => {
            match map_ndjson_output(env, document, &compiled.mapping, layout, options) {
                Ok(binary) => (atoms::ok(), binary.release(env)).encode(env),
                Err(reason) => (atoms::error(), reason).encode(env),
            }
        }
    }
}

fn map_ndjson_output<'a>(
    env: Env<'a>,
    document: Term<'a>,
    mapping: &CompiledMapping,
    layout: &ndjson::CompiledLayout,
    options: Term<'a>,
) -> Result<rustler::OwnedBinary, String> {
    const CONTEXT_ERROR: &str = "NDJSON output requires an ndjson output_context";
    let (flat_keys, format, mapping_config_id, envelope) = decode_output_options(options)?;
    if format != atoms::ndjson() {
        return Err(CONTEXT_ERROR.to_string());
    }
    let mapping_config_id = mapping_config_id
        .decode::<Binary>()
        .map_err(|_| "mapping_config_id must be a UUID string".to_string())?;
    let (id, source_uuid, source_name, ingested_at) = decode_envelope(envelope)?;
    let envelope = ndjson::RowEnvelope {
        id,
        source_uuid,
        source_name,
        ingested_at,
    };
    let nil = atoms::nil().encode(env);
    let mut scratch = mapper::MapScratch::new(mapping, nil);
    mapper::map_values_into(env, document, mapping, flat_keys, nil, &mut scratch);
    ndjson::encode_row(layout, scratch.values(), nil, envelope, mapping_config_id)
}

fn decode_flat_keys(options: Term) -> Result<bool, String> {
    if let Ok(flat_keys) = options.decode::<bool>() {
        return Ok(flat_keys);
    }
    options
        .decode::<(bool, Term)>()
        .map(|(flat_keys, _)| flat_keys)
        .map_err(|_| "mapper options must contain flat_keys".to_string())
}

fn map_clickhouse_output<'a>(
    env: Env<'a>,
    document: Term<'a>,
    mapping: &CompiledMapping,
    layout: &clickhouse_rowbinary::CompiledLayout,
    options: Term<'a>,
) -> Result<rustler::OwnedBinary, String> {
    let (flat_keys, mapping_config_id, envelope) = decode_clickhouse_options(options)?;
    let envelope = decode_clickhouse_envelope(envelope)?;
    let nil = atoms::nil().encode(env);
    let mut scratch = mapper::MapScratch::new(mapping, nil);
    mapper::map_values_into(env, document, mapping, flat_keys, nil, &mut scratch);
    let mut output = clickhouse_rowbinary::BinaryBuilder::new()?;
    clickhouse_rowbinary::append_row(
        &mut output,
        layout,
        scratch.values(),
        envelope,
        mapping_config_id,
    )?;
    output.finish()
}

fn decode_clickhouse_options<'a>(
    options: Term<'a>,
) -> Result<(bool, Binary<'a>, Term<'a>), String> {
    const CONTEXT_ERROR: &str =
        "ClickHouse RowBinary output requires a ch_row_binary output_context";
    let (flat_keys, format, mapping_config_id, envelope) =
        decode_output_options(options).map_err(|_| CONTEXT_ERROR.to_string())?;
    if format != atoms::ch_row_binary() {
        return Err(CONTEXT_ERROR.to_string());
    }
    let mapping_config_id = mapping_config_id
        .decode::<Binary>()
        .map_err(|_| "mapping_config_id must be a pre-encoded 16-byte UUID binary".to_string())?;
    Ok((flat_keys, mapping_config_id, envelope))
}

/// Splits `{flat_keys, {format, mapping_config_id, envelope}}` without
/// interpreting the format-specific parts.
fn decode_output_options<'a>(
    options: Term<'a>,
) -> Result<(bool, rustler::types::atom::Atom, Term<'a>, Term<'a>), String> {
    let (flat_keys, output_context): (bool, Term<'a>) = options
        .decode()
        .map_err(|_| "mapper options must contain flat_keys and output_context".to_string())?;
    let (format, mapping_config_id, envelope): (rustler::types::atom::Atom, Term<'a>, Term<'a>) =
        output_context.decode().map_err(|_| {
            "output_context must be a {format, mapping_config_id, envelope} tuple".to_string()
        })?;
    Ok((flat_keys, format, mapping_config_id, envelope))
}

type Envelope<'a> = (Binary<'a>, Binary<'a>, Binary<'a>, Option<i64>);

fn decode_envelope<'a>(envelope: Term<'a>) -> Result<Envelope<'a>, String> {
    let (id, source_uuid, source_name, ingested_at): (
        Binary<'a>,
        Binary<'a>,
        Binary<'a>,
        Term<'a>,
    ) = envelope.decode().map_err(|_| {
        "row envelope must contain ID, source UUID, source name, and ingested_at".to_string()
    })?;
    let ingested_at = ingested_at
        .decode::<Option<i64>>()
        .map_err(|_| "ingested_at must be nil or an integer Unix timestamp".to_string())?;
    Ok((id, source_uuid, source_name, ingested_at))
}

fn decode_clickhouse_envelope<'a>(
    envelope: Term<'a>,
) -> Result<clickhouse_rowbinary::RowEnvelope<'a>, String> {
    let (id, source_uuid, source_name, ingested_at) = decode_envelope(envelope)?;
    Ok(clickhouse_rowbinary::RowEnvelope {
        id,
        source_uuid,
        source_name,
        ingested_at,
    })
}

fn on_load(env: Env, _info: Term) -> bool {
    env.register::<CompiledMappingResource>().is_ok()
}

rustler::init!("Elixir.Logflare.Mapper.Native", load = on_load);
