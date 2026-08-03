use std::collections::HashMap;

use rustler::types::list::ListIterator;
use rustler::types::map::MapIterator;
use rustler::{Binary, OwnedBinary, Term};

pub type EncodeResult<T> = Result<T, String>;

const INITIAL_ROW_CAPACITY: usize = 3072;
const UUID_BYTE_OFFSETS: [usize; 16] = [0, 2, 4, 6, 9, 11, 14, 16, 19, 21, 24, 26, 28, 30, 32, 34];

const LOG_FIELDS: &[&str] = &[
    "project",
    "trace_id",
    "span_id",
    "trace_flags",
    "severity_text",
    "severity_number_alt",
    "severity_number",
    "service_name",
    "event_message",
    "scope_name",
    "scope_version",
    "scope_schema_url",
    "resource_schema_url",
    "resource_attributes",
    "scope_attributes",
    "log_attributes",
    "timestamp",
];

const METRIC_FIELDS: &[&str] = &[
    "project",
    "time_unix",
    "start_time_unix",
    "metric_name",
    "metric_description",
    "metric_unit",
    "metric_type",
    "service_name",
    "event_message",
    "scope_name",
    "scope_version",
    "scope_schema_url",
    "resource_schema_url",
    "resource_attributes",
    "scope_attributes",
    "attributes",
    "aggregation_temporality",
    "is_monotonic",
    "flags",
    "value",
    "count",
    "sum",
    "min",
    "max",
    "scale",
    "zero_count",
    "positive_offset",
    "negative_offset",
    "bucket_counts",
    "explicit_bounds",
    "positive_bucket_counts",
    "negative_bucket_counts",
    "quantile_values",
    "quantiles",
    "exemplars.filtered_attributes",
    "exemplars.time_unix",
    "exemplars.value",
    "exemplars.span_id",
    "exemplars.trace_id",
    "timestamp",
];

const TRACE_FIELDS: &[&str] = &[
    "project",
    "trace_id",
    "span_id",
    "parent_span_id",
    "trace_state",
    "span_name",
    "span_kind",
    "service_name",
    "event_message",
    "duration",
    "start_time",
    "end_time",
    "status_code",
    "status_message",
    "scope_name",
    "scope_version",
    "resource_attributes",
    "span_attributes",
    "events.timestamp",
    "events.name",
    "events.attributes",
    "links.trace_id",
    "links.span_id",
    "links.trace_state",
    "links.attributes",
    "timestamp",
];

#[derive(Debug, Clone, Copy)]
enum RowType {
    Log,
    Metric,
    Trace,
}

#[derive(Debug)]
pub struct CompiledLayout {
    row_type: RowType,
    field_indices: Box<[usize]>,
}

pub fn compile_layout(
    row_type: &str,
    indices: &HashMap<&str, usize>,
) -> EncodeResult<CompiledLayout> {
    let (row_type, fields) = match row_type {
        "log" => (RowType::Log, LOG_FIELDS),
        "metric" => (RowType::Metric, METRIC_FIELDS),
        "trace" => (RowType::Trace, TRACE_FIELDS),
        _ => return Err(format!("unsupported ClickHouse row type '{row_type}'")),
    };

    let field_indices = fields
        .iter()
        .map(|name| {
            indices.get(*name).copied().ok_or_else(|| {
                format!("compiled mapping is missing required ClickHouse field '{name}'")
            })
        })
        .collect::<EncodeResult<Box<[usize]>>>()?;

    Ok(CompiledLayout {
        row_type,
        field_indices,
    })
}

#[derive(Clone, Copy)]
pub struct RowEnvelope<'a> {
    pub id: Binary<'a>,
    pub source_uuid: Binary<'a>,
    pub source_name: Binary<'a>,
    pub ingested_at: Option<i64>,
}

pub struct BinaryBuilder {
    binary: OwnedBinary,
    len: usize,
}

impl BinaryBuilder {
    pub fn new() -> EncodeResult<Self> {
        let binary = OwnedBinary::new(INITIAL_ROW_CAPACITY)
            .ok_or_else(|| "failed to allocate ClickHouse row output".to_string())?;
        Ok(Self { binary, len: 0 })
    }

    pub fn finish(mut self) -> EncodeResult<OwnedBinary> {
        if self.len == 0 {
            return OwnedBinary::new(0)
                .ok_or_else(|| "failed to allocate empty ClickHouse row output".to_string());
        }
        self.resize(self.len)?;
        Ok(self.binary)
    }

    fn push(&mut self, value: u8) -> EncodeResult<()> {
        let end = self.reserve(1)?;
        self.binary.as_mut_slice()[self.len] = value;
        self.len = end;
        Ok(())
    }

    fn extend_from_slice(&mut self, value: &[u8]) -> EncodeResult<()> {
        let end = self.reserve(value.len())?;
        self.binary.as_mut_slice()[self.len..end].copy_from_slice(value);
        self.len = end;
        Ok(())
    }

    fn reserve(&mut self, additional: usize) -> EncodeResult<usize> {
        let required = self
            .len
            .checked_add(additional)
            .ok_or_else(|| "ClickHouse row output size overflow".to_string())?;
        if required <= self.binary.len() {
            return Ok(required);
        }

        let capacity = self
            .binary
            .len()
            .saturating_mul(2)
            .max(required)
            .max(INITIAL_ROW_CAPACITY);
        self.resize(capacity)?;
        Ok(required)
    }

    fn resize(&mut self, size: usize) -> EncodeResult<()> {
        if self.binary.realloc(size) {
            return Ok(());
        }

        let copy_len = self.len.min(size);
        let mut replacement = OwnedBinary::new(size)
            .ok_or_else(|| "failed to resize ClickHouse row output".to_string())?;
        let initialized = &self.binary.as_mut_slice()[..copy_len];
        replacement.as_mut_slice()[..copy_len].copy_from_slice(initialized);
        self.binary = replacement;
        Ok(())
    }
}

struct RowValues<'values, 'env> {
    values: &'values [Term<'env>],
    layout: &'values [usize],
    cursor: usize,
}

impl<'values, 'env> RowValues<'values, 'env> {
    fn new(values: &'values [Term<'env>], layout: &'values [usize]) -> Self {
        Self {
            values,
            layout,
            cursor: 0,
        }
    }

    fn next(&mut self, name: &str) -> EncodeResult<Term<'env>> {
        let field_index = self
            .layout
            .get(self.cursor)
            .ok_or_else(|| format!("compiled mapping is missing ClickHouse field '{name}'"))?;
        self.cursor += 1;
        self.values
            .get(*field_index)
            .copied()
            .ok_or_else(|| format!("compiled mapping did not produce ClickHouse field '{name}'"))
    }
}

pub fn append_row(
    output: &mut BinaryBuilder,
    layout: &CompiledLayout,
    values: &[Term],
    envelope: RowEnvelope,
    mapping_config_id: Binary,
) -> EncodeResult<()> {
    match layout.row_type {
        RowType::Log => append_log(
            output,
            &layout.field_indices,
            values,
            envelope,
            mapping_config_id,
        ),
        RowType::Metric => append_metric(
            output,
            &layout.field_indices,
            values,
            envelope,
            mapping_config_id,
        ),
        RowType::Trace => append_trace(
            output,
            &layout.field_indices,
            values,
            envelope,
            mapping_config_id,
        ),
    }
}

fn append_log(
    output: &mut BinaryBuilder,
    layout: &[usize],
    values: &[Term],
    envelope: RowEnvelope,
    mapping_config_id: Binary,
) -> EncodeResult<()> {
    let mut values = RowValues::new(values, layout);
    encode_envelope(
        output,
        envelope.id,
        envelope.source_uuid,
        envelope.source_name,
    )?;

    encode_string(output, values.next("project")?)?;
    encode_string(output, values.next("trace_id")?)?;
    encode_string(output, values.next("span_id")?)?;
    encode_uint8(output, values.next("trace_flags")?)?;
    encode_string(output, values.next("severity_text")?)?;

    let severity_alt = decode_u64(values.next("severity_number_alt")?)?;
    let mapped_severity = values.next("severity_number")?;
    let severity = if severity_alt > 0 {
        severity_alt
    } else {
        decode_u64(mapped_severity)?
    };
    output.push(to_u8(severity, "severity_number")?)?;

    encode_string(output, values.next("service_name")?)?;
    encode_string(output, values.next("event_message")?)?;
    encode_string(output, values.next("scope_name")?)?;
    encode_string(output, values.next("scope_version")?)?;
    encode_string(output, values.next("scope_schema_url")?)?;
    encode_string(output, values.next("resource_schema_url")?)?;
    encode_map_string_string(output, values.next("resource_attributes")?)?;
    encode_map_string_string(output, values.next("scope_attributes")?)?;
    encode_map_string_string(output, values.next("log_attributes")?)?;

    encode_suffix(
        output,
        mapping_config_id,
        envelope.ingested_at,
        values.next("timestamp")?,
    )
}

fn append_metric(
    output: &mut BinaryBuilder,
    layout: &[usize],
    values: &[Term],
    envelope: RowEnvelope,
    mapping_config_id: Binary,
) -> EncodeResult<()> {
    let mut values = RowValues::new(values, layout);
    encode_envelope(
        output,
        envelope.id,
        envelope.source_uuid,
        envelope.source_name,
    )?;

    encode_string(output, values.next("project")?)?;
    encode_nullable_int64(output, values.next("time_unix")?)?;
    encode_nullable_int64(output, values.next("start_time_unix")?)?;
    encode_string(output, values.next("metric_name")?)?;
    encode_string(output, values.next("metric_description")?)?;
    encode_string(output, values.next("metric_unit")?)?;
    encode_int8(output, values.next("metric_type")?)?;
    encode_string(output, values.next("service_name")?)?;
    encode_string(output, values.next("event_message")?)?;
    encode_string(output, values.next("scope_name")?)?;
    encode_string(output, values.next("scope_version")?)?;
    encode_string(output, values.next("scope_schema_url")?)?;
    encode_string(output, values.next("resource_schema_url")?)?;
    encode_map_string_string(output, values.next("resource_attributes")?)?;
    encode_map_string_string(output, values.next("scope_attributes")?)?;
    encode_map_string_string(output, values.next("attributes")?)?;
    encode_string(output, values.next("aggregation_temporality")?)?;
    encode_bool(output, values.next("is_monotonic")?)?;
    encode_uint32(output, values.next("flags")?)?;
    encode_float64(output, values.next("value")?)?;
    encode_uint64(output, values.next("count")?)?;
    encode_float64(output, values.next("sum")?)?;
    encode_float64(output, values.next("min")?)?;
    encode_float64(output, values.next("max")?)?;
    encode_int32(output, values.next("scale")?)?;
    encode_uint64(output, values.next("zero_count")?)?;
    encode_int32(output, values.next("positive_offset")?)?;
    encode_int32(output, values.next("negative_offset")?)?;
    encode_array_uint64(output, values.next("bucket_counts")?)?;
    encode_array_float64(output, values.next("explicit_bounds")?)?;
    encode_array_uint64(output, values.next("positive_bucket_counts")?)?;
    encode_array_uint64(output, values.next("negative_bucket_counts")?)?;
    encode_array_float64(output, values.next("quantile_values")?)?;
    encode_array_float64(output, values.next("quantiles")?)?;
    encode_array_map_string_string(output, values.next("exemplars.filtered_attributes")?)?;
    encode_array_int64(output, values.next("exemplars.time_unix")?)?;
    encode_array_float64(output, values.next("exemplars.value")?)?;
    encode_array_string(output, values.next("exemplars.span_id")?)?;
    encode_array_string(output, values.next("exemplars.trace_id")?)?;

    encode_suffix(
        output,
        mapping_config_id,
        envelope.ingested_at,
        values.next("timestamp")?,
    )
}

fn append_trace(
    output: &mut BinaryBuilder,
    layout: &[usize],
    values: &[Term],
    envelope: RowEnvelope,
    mapping_config_id: Binary,
) -> EncodeResult<()> {
    let mut values = RowValues::new(values, layout);
    encode_envelope(
        output,
        envelope.id,
        envelope.source_uuid,
        envelope.source_name,
    )?;

    encode_string(output, values.next("project")?)?;
    encode_string(output, values.next("trace_id")?)?;
    encode_string(output, values.next("span_id")?)?;
    encode_string(output, values.next("parent_span_id")?)?;
    encode_string(output, values.next("trace_state")?)?;
    encode_string(output, values.next("span_name")?)?;
    encode_string(output, values.next("span_kind")?)?;
    encode_string(output, values.next("service_name")?)?;
    encode_string(output, values.next("event_message")?)?;

    let mut duration = decode_u64(values.next("duration")?)?;
    let start_time = decode_i64(values.next("start_time")?);
    let end_time = decode_i64(values.next("end_time")?);
    if duration == 0 {
        if let (Ok(start_time), Ok(end_time)) = (start_time, end_time) {
            if end_time > start_time {
                duration = end_time.abs_diff(start_time);
            }
        }
    }
    output.extend_from_slice(&duration.to_le_bytes())?;

    encode_string(output, values.next("status_code")?)?;
    encode_string(output, values.next("status_message")?)?;
    encode_string(output, values.next("scope_name")?)?;
    encode_string(output, values.next("scope_version")?)?;
    encode_map_string_string(output, values.next("resource_attributes")?)?;
    encode_map_string_string(output, values.next("span_attributes")?)?;
    encode_array_int64(output, values.next("events.timestamp")?)?;
    encode_array_string(output, values.next("events.name")?)?;
    encode_array_map_string_string(output, values.next("events.attributes")?)?;
    encode_array_string(output, values.next("links.trace_id")?)?;
    encode_array_string(output, values.next("links.span_id")?)?;
    encode_array_string(output, values.next("links.trace_state")?)?;
    encode_array_map_string_string(output, values.next("links.attributes")?)?;

    encode_suffix(
        output,
        mapping_config_id,
        envelope.ingested_at,
        values.next("timestamp")?,
    )
}

fn encode_envelope(
    output: &mut BinaryBuilder,
    id: Binary,
    source_uuid: Binary,
    source_name: Binary,
) -> EncodeResult<()> {
    encode_uuid(output, id.as_slice())?;
    encode_bytes(output, source_uuid.as_slice())?;
    encode_bytes(output, source_name.as_slice())?;
    Ok(())
}

fn encode_suffix(
    output: &mut BinaryBuilder,
    mapping_config_id: Binary,
    ingested_at: Option<i64>,
    timestamp: Term,
) -> EncodeResult<()> {
    if mapping_config_id.len() != 16 {
        return Err("mapping config ID must be a 16-byte encoded UUID".to_string());
    }
    output.extend_from_slice(mapping_config_id.as_slice())?;
    match ingested_at {
        Some(value) => {
            output.push(0)?;
            output.extend_from_slice(&value.to_le_bytes())?;
        }
        None => output.push(1)?,
    }
    encode_int64(output, timestamp)
}

fn encode_uuid(output: &mut BinaryBuilder, value: &[u8]) -> EncodeResult<()> {
    let mut raw = [0_u8; 16];

    if value.len() == 36
        && value[8] == b'-'
        && value[13] == b'-'
        && value[18] == b'-'
        && value[23] == b'-'
    {
        for (destination, source) in raw.iter_mut().zip(UUID_BYTE_OFFSETS) {
            *destination = decode_hex_pair(value[source], value[source + 1])?;
        }
    } else if value.len() == 32 {
        for (destination, pair) in raw.iter_mut().zip(value.chunks_exact(2)) {
            *destination = decode_hex_pair(pair[0], pair[1])?;
        }
    } else {
        decode_uuid_flexible(value, &mut raw)?;
    }

    raw[..8].reverse();
    raw[8..].reverse();
    output.extend_from_slice(&raw)
}

fn decode_uuid_flexible(value: &[u8], raw: &mut [u8; 16]) -> EncodeResult<()> {
    let mut raw_index = 0;
    let mut high_nibble = None;

    for byte in value {
        if *byte == b'-' {
            continue;
        }
        let nibble = decode_hex(*byte).ok_or_else(|| "invalid event UUID".to_string())?;
        match high_nibble.take() {
            Some(high) => {
                if raw_index >= raw.len() {
                    return Err("invalid event UUID length".to_string());
                }
                raw[raw_index] = (high << 4) | nibble;
                raw_index += 1;
            }
            None => high_nibble = Some(nibble),
        }
    }

    if raw_index == raw.len() && high_nibble.is_none() {
        Ok(())
    } else {
        Err("invalid event UUID length".to_string())
    }
}

#[inline]
fn decode_hex_pair(high: u8, low: u8) -> EncodeResult<u8> {
    let high = decode_hex(high).ok_or_else(|| "invalid event UUID".to_string())?;
    let low = decode_hex(low).ok_or_else(|| "invalid event UUID".to_string())?;
    Ok((high << 4) | low)
}

fn decode_hex(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

fn encode_string(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let binary = value
        .decode::<Binary>()
        .map_err(|_| "mapped string field is not a binary".to_string())?;
    encode_bytes(output, binary.as_slice())
}

fn encode_bytes(output: &mut BinaryBuilder, value: &[u8]) -> EncodeResult<()> {
    encode_varuint(output, value.len() as u64)?;
    output.extend_from_slice(value)
}

fn encode_varuint(output: &mut BinaryBuilder, mut value: u64) -> EncodeResult<()> {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        output.push(byte)?;
        if value == 0 {
            return Ok(());
        }
    }
}

fn encode_bool(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let value = value
        .decode::<bool>()
        .map_err(|_| "mapped boolean field is not a boolean".to_string())?;
    output.push(u8::from(value))
}

fn encode_uint8(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let value = decode_u64(value)?;
    output.push(to_u8(value, "UInt8")?)
}

fn encode_uint32(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let value = decode_u64(value)?;
    let value =
        u32::try_from(value).map_err(|_| "mapped UInt32 field is out of range".to_string())?;
    output.extend_from_slice(&value.to_le_bytes())
}

fn encode_uint64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    output.extend_from_slice(&decode_u64(value)?.to_le_bytes())
}

fn encode_int8(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let value = decode_i64(value)?;
    let value = i8::try_from(value).map_err(|_| "mapped Int8 field is out of range".to_string())?;
    output.push(value as u8)
}

fn encode_int32(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let value = decode_i64(value)?;
    let value =
        i32::try_from(value).map_err(|_| "mapped Int32 field is out of range".to_string())?;
    output.extend_from_slice(&value.to_le_bytes())
}

fn encode_int64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    output.extend_from_slice(&decode_i64(value)?.to_le_bytes())
}

fn encode_nullable_int64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    match value.decode::<Option<i64>>() {
        Ok(Some(value)) => {
            output.push(0)?;
            output.extend_from_slice(&value.to_le_bytes())
        }
        Ok(None) => output.push(1),
        Err(_) => Err("mapped nullable Int64 field is neither nil nor an integer".to_string()),
    }
}

fn encode_float64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let value = if let Ok(value) = value.decode::<f64>() {
        value
    } else if let Ok(value) = value.decode::<i64>() {
        value as f64
    } else {
        return Err("mapped Float64 field is not numeric".to_string());
    };
    output.extend_from_slice(&value.to_le_bytes())
}

fn decode_u64(value: Term) -> EncodeResult<u64> {
    if let Ok(value) = value.decode::<u64>() {
        Ok(value)
    } else if let Ok(value) = value.decode::<i64>() {
        u64::try_from(value).map_err(|_| "mapped unsigned field is negative".to_string())
    } else {
        Err("mapped unsigned field is not an integer".to_string())
    }
}

fn decode_i64(value: Term) -> EncodeResult<i64> {
    value
        .decode::<i64>()
        .map_err(|_| "mapped signed field is not an integer".to_string())
}

fn to_u8(value: u64, field: &str) -> EncodeResult<u8> {
    u8::try_from(value).map_err(|_| format!("mapped {field} field is out of range"))
}

fn encode_map_string_string(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let size = value
        .map_size()
        .map_err(|_| "mapped Map(String, String) field is not a map".to_string())?;
    encode_varuint(output, size as u64)?;
    let entries = MapIterator::new(value)
        .ok_or_else(|| "mapped Map(String, String) field is not a map".to_string())?;

    for (key, value) in entries {
        encode_string(output, key)?;
        encode_string(output, value)?;
    }
    Ok(())
}

fn list<'a>(value: Term<'a>) -> EncodeResult<(usize, ListIterator<'a>)> {
    let length = value
        .list_length()
        .map_err(|_| "mapped array field is not a proper list".to_string())?;
    let values = value
        .decode::<ListIterator>()
        .map_err(|_| "mapped array field is not a proper list".to_string())?;
    Ok((length, values))
}

fn encode_array_string(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let (length, values) = list(value)?;
    encode_varuint(output, length as u64)?;
    for value in values {
        encode_string(output, value)?;
    }
    Ok(())
}

fn encode_array_uint64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let (length, values) = list(value)?;
    encode_varuint(output, length as u64)?;
    for value in values {
        encode_uint64(output, value)?;
    }
    Ok(())
}

fn encode_array_int64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let (length, values) = list(value)?;
    encode_varuint(output, length as u64)?;
    for value in values {
        encode_int64(output, value)?;
    }
    Ok(())
}

fn encode_array_float64(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let (length, values) = list(value)?;
    encode_varuint(output, length as u64)?;
    for value in values {
        encode_float64(output, value)?;
    }
    Ok(())
}

fn encode_array_map_string_string(output: &mut BinaryBuilder, value: Term) -> EncodeResult<()> {
    let (length, values) = list(value)?;
    encode_varuint(output, length as u64)?;
    for value in values {
        encode_map_string_string(output, value)?;
    }
    Ok(())
}
