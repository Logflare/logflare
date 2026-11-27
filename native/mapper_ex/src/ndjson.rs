//! NDJSON output: one JSON object per mapped document, newline terminated.
//!
//! The row layout keeps every compiled field in config order and resolves the
//! indices needed by the derived-field rules at compile time, so writing a row
//! is a single pass over the mapped values.
//!
//! `NdjsonOptions` adapt the layout for consumers that cannot store it as is
//! (e.g. BigQuery): column names are rewritten once here, at compile time, so a
//! collision is a compile error rather than a value lost on every row.

use std::collections::{HashMap, HashSet};

use rustler::{Binary, ListIterator, OwnedBinary, Term};
use serde::ser::SerializeMap;
use serde::{Serialize, Serializer};

use crate::mapper::JsonTerm;
use crate::mapping::{CompiledField, FieldType};
use crate::output::{decode_u64, EncodeResult, RowEnvelope};

const INITIAL_ROW_CAPACITY: usize = 2048;

#[derive(Debug)]
pub struct CompiledLayout {
    columns: Box<[Column]>,
    derived: Derived,
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct NdjsonOptions {
    /// Replace `.` with `_` in column names.
    pub underscore_names: bool,
    /// Write each element of an `array_flat_map` column as a JSON string.
    pub json_string_elements: bool,
}

impl NdjsonOptions {
    /// Decodes the `column_names` and `nested_arrays` output options; a missing
    /// option keeps the default layout.
    pub fn decode(column_names: Option<&str>, nested_arrays: Option<&str>) -> EncodeResult<Self> {
        let underscore_names = match column_names {
            None | Some("dotted") => false,
            Some("underscore") => true,
            Some(other) => return Err(format!("unsupported NDJSON column_names '{other}'")),
        };
        let json_string_elements = match nested_arrays {
            None | Some("native") => false,
            Some("json_strings") => true,
            Some(other) => return Err(format!("unsupported NDJSON nested_arrays '{other}'")),
        };

        Ok(Self {
            underscore_names,
            json_string_elements,
        })
    }

    fn column_name(&self, field: &CompiledField) -> String {
        if self.underscore_names {
            field.name.replace('.', "_")
        } else {
            field.name.clone()
        }
    }
}

#[derive(Debug)]
struct Column {
    name: String,
    index: usize,
    /// Enum8 value -> label. The label is undefined when two labels share a
    /// value; `decode_enum_values` does not enforce reverse uniqueness yet.
    enum_labels: Option<HashMap<i64, String>>,
    json_elements: bool,
}

#[derive(Debug)]
enum Derived {
    None,
    Log {
        severity_alt: usize,
        severity_number: usize,
    },
    /// `duration_precision` is the `start_time` field's DateTime64
    /// precision: the unit `duration` is derived and emitted in.
    Trace {
        duration: usize,
        start_time: usize,
        end_time: usize,
        duration_precision: u8,
    },
}

/// Keys written by `write_row` before the mapped columns; a config field with
/// one of these names would produce a duplicate JSON key.
const ENVELOPE_KEYS: [&str; 5] = [
    "id",
    "source_uuid",
    "source_name",
    "mapping_config_id",
    "ingested_at",
];

pub fn compile_layout(
    row_type: &str,
    fields: &[CompiledField],
    options: NdjsonOptions,
) -> EncodeResult<CompiledLayout> {
    if let Some(name) = fields
        .iter()
        .map(|field| options.column_name(field))
        .find(|name| ENVELOPE_KEYS.contains(&name.as_str()))
    {
        return Err(format!(
            "field name '{name}' is reserved for the NDJSON envelope"
        ));
    }

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
        "trace" => {
            let start_time = index_of("start_time")?;
            let duration_precision = match fields[start_time].field_type {
                FieldType::DateTime64 { precision } => precision,
                _ => return Err("NDJSON trace field 'start_time' must be a datetime64".to_string()),
            };
            (
                Derived::Trace {
                    duration: index_of("duration")?,
                    start_time,
                    end_time: index_of("end_time")?,
                    duration_precision,
                },
                None,
            )
        }
        _ => return Err(format!("unsupported NDJSON row type '{row_type}'")),
    };

    let mut names = HashSet::new();
    let mut columns = Vec::with_capacity(fields.len());

    for (index, field) in fields.iter().enumerate() {
        if Some(field.name.as_str()) == hidden {
            continue;
        }

        let name = options.column_name(field);

        if !names.insert(name.clone()) {
            return Err(format!(
                "NDJSON column name '{name}' is produced by more than one field"
            ));
        }

        columns.push(Column {
            name,
            index,
            enum_labels: field.enum8_data.as_ref().map(|data| {
                data.value_map
                    .iter()
                    .map(|(label, value)| (i64::from(*value), label.clone()))
                    .collect()
            }),
            json_elements: options.json_string_elements
                && field.field_type == FieldType::ArrayFlatMap,
        });
    }

    Ok(CompiledLayout {
        columns: columns.into_boxed_slice(),
        derived,
    })
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

        match derived_value(layout, column.index, values)? {
            Some(number) => object.serialize_entry(&column.name, &number),
            None if column.enum_labels.is_some() => {
                object.serialize_entry(&column.name, &enum_label(column, value))
            }
            None if column.json_elements => {
                object.serialize_entry(&column.name, &JsonStringElements { value, nil })
            }
            None => object.serialize_entry(&column.name, &JsonTerm { value, nil }),
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
            duration_precision,
        } if index == duration => {
            let explicit = decode_u64(values[duration], "duration")?;
            let start = values[start_time].decode::<i64>();
            let end = values[end_time].decode::<i64>();
            Ok(Some(crate::derive::duration(
                explicit,
                start,
                end,
                duration_precision,
            )))
        }
        _ => Ok(None),
    }
}

/// A list written as one JSON string per element, so an `array_flat_map`
/// element (a flat attribute map) becomes a JSON object string. Values that are
/// not lists are written as `JsonTerm` would write them.
struct JsonStringElements<'a> {
    value: Term<'a>,
    nil: Term<'a>,
}

impl Serialize for JsonStringElements<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use serde::ser::SerializeSeq;

        let Ok(items) = self.value.decode::<ListIterator>() else {
            return JsonTerm {
                value: self.value,
                nil: self.nil,
            }
            .serialize(serializer);
        };

        let mut sequence = serializer.serialize_seq(None)?;
        for value in items {
            let json = serde_json::to_string(&JsonTerm {
                value,
                nil: self.nil,
            })
            .map_err(serde::ser::Error::custom)?;
            sequence.serialize_element(&json)?;
        }
        sequence.end()
    }
}

fn enum_label<'a>(column: &'a Column, value: Term) -> Option<&'a str> {
    let labels = column.enum_labels.as_ref()?;
    let value = value.decode::<i64>().ok()?;
    labels.get(&value).map(String::as_str)
}

fn utf8<'a>(binary: Binary<'a>, field: &str) -> EncodeResult<&'a str> {
    std::str::from_utf8(binary.as_slice())
        .map_err(|_| format!("NDJSON envelope {field} must be UTF-8"))
}
