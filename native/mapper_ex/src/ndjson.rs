//! NDJSON output: one JSON object per mapped document, newline terminated.
//!
//! The row layout keeps every compiled field in config order and resolves the
//! indices needed by the derived-field rules at compile time, so writing a row
//! is a single pass over the mapped values.

use std::collections::HashMap;

use rustler::{Binary, OwnedBinary, Term};
use serde::ser::SerializeMap;
use serde::Serializer;

use crate::mapper::JsonTerm;
use crate::mapping::CompiledField;

pub type EncodeResult<T> = Result<T, String>;

const INITIAL_ROW_CAPACITY: usize = 2048;

#[derive(Debug)]
pub struct CompiledLayout {
    columns: Box<[Column]>,
    derived: Derived,
}

#[derive(Debug)]
struct Column {
    name: String,
    index: usize,
    enum_labels: Option<HashMap<i64, String>>,
}

#[derive(Debug)]
enum Derived {
    None,
    Log {
        severity_alt: usize,
        severity_number: usize,
    },
    Trace {
        duration: usize,
        start_time: usize,
        end_time: usize,
    },
}

#[derive(Clone, Copy)]
pub struct RowEnvelope<'a> {
    pub id: Binary<'a>,
    pub source_uuid: Binary<'a>,
    pub source_name: Binary<'a>,
    pub ingested_at: Option<i64>,
}

pub fn compile_layout(row_type: &str, fields: &[CompiledField]) -> EncodeResult<CompiledLayout> {
    let index_of = |name: &str| {
        fields
            .iter()
            .position(|field| field.name == name)
            .ok_or_else(|| format!("compiled mapping is missing required NDJSON field '{name}'"))
    };

    let (derived, hidden) = match row_type {
        "log" => (
            Derived::Log {
                severity_alt: index_of("severity_number_alt")?,
                severity_number: index_of("severity_number")?,
            },
            Some("severity_number_alt"),
        ),
        "metric" => (Derived::None, None),
        "trace" => (
            Derived::Trace {
                duration: index_of("duration")?,
                start_time: index_of("start_time")?,
                end_time: index_of("end_time")?,
            },
            None,
        ),
        _ => return Err(format!("unsupported NDJSON row type '{row_type}'")),
    };

    let columns = fields
        .iter()
        .enumerate()
        .filter(|(_, field)| Some(field.name.as_str()) != hidden)
        .map(|(index, field)| Column {
            name: field.name.clone(),
            index,
            enum_labels: field.enum8_data.as_ref().map(|data| {
                data.value_map
                    .iter()
                    .map(|(label, value)| (i64::from(*value), label.clone()))
                    .collect()
            }),
        })
        .collect();

    Ok(CompiledLayout { columns, derived })
}

pub fn encode_row<'a>(
    layout: &CompiledLayout,
    values: &[Term<'a>],
    nil: Term<'a>,
    envelope: RowEnvelope<'a>,
    mapping_config_id: Binary<'a>,
) -> EncodeResult<OwnedBinary> {
    let mut buffer = Vec::with_capacity(INITIAL_ROW_CAPACITY);
    write_row(
        &mut buffer,
        layout,
        values,
        nil,
        envelope,
        mapping_config_id,
    )?;
    buffer.push(b'\n');

    let mut binary = OwnedBinary::new(buffer.len())
        .ok_or_else(|| "failed to allocate NDJSON row output".to_string())?;
    binary.as_mut_slice().copy_from_slice(&buffer);
    Ok(binary)
}

fn write_row<'a>(
    buffer: &mut Vec<u8>,
    layout: &CompiledLayout,
    values: &[Term<'a>],
    nil: Term<'a>,
    envelope: RowEnvelope<'a>,
    mapping_config_id: Binary<'a>,
) -> EncodeResult<()> {
    let mut serializer = serde_json::Serializer::new(buffer);
    let mut object = serializer
        .serialize_map(None)
        .map_err(|error| error.to_string())?;

    let id = utf8(envelope.id, "id")?;
    let source_uuid = utf8(envelope.source_uuid, "source_uuid")?;
    let source_name = utf8(envelope.source_name, "source_name")?;
    let mapping_config_id = utf8(mapping_config_id, "mapping_config_id")?;

    object
        .serialize_entry("id", id)
        .and_then(|_| object.serialize_entry("source_uuid", source_uuid))
        .and_then(|_| object.serialize_entry("source_name", source_name))
        .and_then(|_| object.serialize_entry("mapping_config_id", mapping_config_id))
        .and_then(|_| object.serialize_entry("ingested_at", &envelope.ingested_at))
        .map_err(|error| error.to_string())?;

    for column in layout.columns.iter() {
        let value = values.get(column.index).copied().ok_or_else(|| {
            format!(
                "compiled mapping did not produce NDJSON field '{}'",
                column.name
            )
        })?;

        // Try derived value, then enum mapping, fallback to generic conversion
        match derived_value(layout, column.index, values)? {
            Some(number) => object.serialize_entry(&column.name, &number),
            None => match enum_label(column, value) {
                Some(label) => object.serialize_entry(&column.name, label),
                None => object.serialize_entry(&column.name, &JsonTerm { value, nil }),
            },
        }
        .map_err(|error| error.to_string())?;
    }

    object.end().map_err(|error| error.to_string())
}

fn derived_value(
    layout: &CompiledLayout,
    index: usize,
    values: &[Term],
) -> EncodeResult<Option<u64>> {
    match layout.derived {
        Derived::Log {
            severity_alt,
            severity_number,
        } if index == severity_number => {
            let alt = decode_u64(values[severity_alt], "severity_number_alt")?;
            let mapped = decode_u64(values[severity_number], "severity_number")?;
            Ok(Some(crate::derive::severity_number(alt, mapped)))
        }
        Derived::Trace {
            duration,
            start_time,
            end_time,
        } if index == duration => {
            let explicit = decode_u64(values[duration], "duration")?;
            let start = values[start_time].decode::<i64>();
            let end = values[end_time].decode::<i64>();
            Ok(Some(crate::derive::duration(explicit, start, end)))
        }
        _ => Ok(None),
    }
}

fn enum_label<'a>(column: &'a Column, value: Term) -> Option<&'a str> {
    let labels = column.enum_labels.as_ref()?;
    let value = value.decode::<i64>().ok()?;
    labels.get(&value).map(String::as_str)
}

fn decode_u64(value: Term, field: &str) -> EncodeResult<u64> {
    if let Ok(value) = value.decode::<u64>() {
        return Ok(value);
    }
    match value.decode::<i64>() {
        Ok(value) => {
            u64::try_from(value).map_err(|_| format!("mapped field '{field}' is negative"))
        }
        Err(_) => Err(format!("mapped field '{field}' is not an integer")),
    }
}

fn utf8<'a>(binary: Binary<'a>, field: &str) -> EncodeResult<&'a str> {
    std::str::from_utf8(binary.as_slice())
        .map_err(|_| format!("NDJSON envelope {field} must be UTF-8"))
}
