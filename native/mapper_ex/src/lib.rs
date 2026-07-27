mod clickhouse_rowbinary;
mod coerce;
mod mapper;
mod mapping;
mod path;
mod query;
mod string_filters;

use rustler::{Atom, Binary, Encoder, Env, NewBinary, NifResult, Resource, ResourceArc, Term};

use mapping::CompiledMapping;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        log,
        metric,
        trace,
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

/// Maps one document and directly encodes a complete ClickHouse RowBinary row.
/// This avoids materializing the mapped output as an intermediate BEAM map.
#[rustler::nif]
fn map_and_encode_clickhouse<'a>(
    env: Env<'a>,
    document: Term<'a>,
    compiled: ResourceArc<CompiledMappingResource>,
    event_type: Atom,
    mapping_config_id: Binary<'a>,
) -> Term<'a> {
    let encoded = (|| {
        let (layout, row_type) = if event_type == atoms::log() {
            (
                compiled.mapping.clickhouse_layouts.log.as_deref(),
                ClickHouseRowType::Log,
            )
        } else if event_type == atoms::metric() {
            (
                compiled.mapping.clickhouse_layouts.metric.as_deref(),
                ClickHouseRowType::Metric,
            )
        } else if event_type == atoms::trace() {
            (
                compiled.mapping.clickhouse_layouts.trace.as_deref(),
                ClickHouseRowType::Trace,
            )
        } else {
            return Err("unsupported ClickHouse event type".to_string());
        };
        let layout = layout.ok_or_else(|| {
            "compiled mapping does not contain the required ClickHouse fields".to_string()
        })?;

        let (body, envelope): (Term<'a>, Term<'a>) = document
            .decode()
            .map_err(|_| "document must contain a body and row envelope".to_string())?;
        let envelope = decode_clickhouse_envelope(env, envelope)?;
        let nil = atoms::nil().encode(env);
        let mut scratch = mapper::MapScratch::new(&compiled.mapping, nil);
        mapper::map_values_into(env, body, &compiled.mapping, false, nil, &mut scratch);
        let mut output = clickhouse_rowbinary::BinaryBuilder::new(row_type.initial_capacity())?;

        match row_type {
            ClickHouseRowType::Log => clickhouse_rowbinary::append_log(
                &mut output,
                layout,
                scratch.values(),
                envelope,
                mapping_config_id,
            )?,
            ClickHouseRowType::Metric => clickhouse_rowbinary::append_metric(
                &mut output,
                layout,
                scratch.values(),
                envelope,
                mapping_config_id,
            )?,
            ClickHouseRowType::Trace => clickhouse_rowbinary::append_trace(
                &mut output,
                layout,
                scratch.values(),
                envelope,
                mapping_config_id,
            )?,
        }

        output.finish()
    })();

    match encoded {
        Ok(binary) => (atoms::ok(), binary.release(env)).encode(env),
        Err(reason) => (atoms::error(), reason).encode(env),
    }
}

#[derive(Clone, Copy)]
enum ClickHouseRowType {
    Log,
    Metric,
    Trace,
}

impl ClickHouseRowType {
    fn initial_capacity(self) -> usize {
        match self {
            Self::Log => 2048,
            Self::Metric => 2560,
            Self::Trace => 2304,
        }
    }
}

fn decode_clickhouse_envelope<'a>(
    env: Env<'a>,
    envelope: Term<'a>,
) -> Result<clickhouse_rowbinary::RowEnvelope<'a>, String> {
    let (id, source_uuid, source_name, ingested_at): (
        Binary<'a>,
        Binary<'a>,
        Binary<'a>,
        Term<'a>,
    ) = envelope.decode().map_err(|_| {
        "row envelope must contain ID, source UUID, source name, and ingested_at".to_string()
    })?;
    let ingested_at = if ingested_at == atoms::nil().encode(env) {
        None
    } else {
        Some(
            ingested_at
                .decode::<i64>()
                .map_err(|_| "ingested_at must be nil or Unix microseconds".to_string())?,
        )
    };

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
