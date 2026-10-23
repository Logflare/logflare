use std::{collections::HashSet, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray, ListArray, RecordBatch},
    datatypes::{DataType, Field, Fields, Schema, SchemaRef, TimeUnit},
    error::ArrowError,
    ipc::{
        writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
        CompressionType,
    },
    json::{reader::infer_json_schema, ReaderBuilder},
};
use rustler::{Atom, Binary, Env, Error, NewBinary, NifResult, OwnedBinary};
use serde_json::{Map, Value};

rustler::atoms! {
    zstd,
    lz4,
    none,
    string,
    integer,
    float,
    boolean,
    timestamp,
    string_map,
    integer_array,
    float_array,
    string_array,
    json_array,
}

fn atom_to_compression(atom: Atom) -> NifResult<Option<CompressionType>> {
    if atom == zstd() {
        Ok(Some(CompressionType::ZSTD))
    } else if atom == lz4() {
        Ok(Some(CompressionType::LZ4_FRAME))
    } else if atom == none() {
        Ok(None)
    } else {
        Err(Error::BadArg)
    }
}

fn build_write_options(compression: Option<CompressionType>) -> IpcWriteOptions {
    IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap()
}

#[rustler::nif(schedule = "DirtyCpu")]
fn get_ipc_bytes(
    env: Env,
    dataframe_json: String,
    compression: Atom,
) -> NifResult<(Binary, Vec<Binary>)> {
    let compression_type = atom_to_compression(compression)?;
    let byte_data = dataframe_json.as_bytes();

    let inferred_schema = {
        let (inferred_schema, _) = infer_json_schema(byte_data, None).unwrap();

        let new_fields: Vec<Arc<Field>> = inferred_schema
            .fields()
            .iter()
            .map(|field| {
                if field.name() == "timestamp" {
                    // this is a hardcoded field and the only one that we know is timestamp
                    Field::new(
                        "timestamp",
                        DataType::Timestamp(TimeUnit::Microsecond, Some("+00".into())),
                        false,
                    )
                    .with_metadata(field.metadata().clone())
                    .into()
                } else {
                    // Keep all other fields exactly as they are
                    field.clone()
                }
            })
            .collect();

        Arc::new(Schema::new(new_fields))
    };

    let write_options = build_write_options(compression_type);

    let ipc_schema_bytes = {
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let data_gen = IpcDataGenerator::default();
        let schema_data = data_gen.schema_to_bytes_with_dictionary_tracker(
            &inferred_schema,
            &mut dictionary_tracker,
            &write_options,
        );

        let mut ipc_schema_bytes = Vec::new();

        write_message(&mut ipc_schema_bytes, schema_data, &write_options).unwrap();

        ipc_schema_bytes
    };

    // Determine max_chunksize of the record batches. Because max size of
    // AppendRowsRequest is 10 MB, we need to split the table if it's too big.
    // See: https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#appendrowsrequest
    const max_request_bytes: usize = {
        let base: usize = 2;
        8 * base.pow(20) // 8 MB
    };

    let json_reader = ReaderBuilder::new(inferred_schema.clone())
        .with_batch_size(max_request_bytes)
        .build(byte_data)
        .unwrap();

    let mut result = Vec::new();

    for batch_record_result in json_reader {
        let batch_record = batch_record_result.unwrap();
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let data_gen = IpcDataGenerator::default();
        let (_, encoded_batch) = data_gen
            .encoded_batch(&batch_record, &mut dictionary_tracker, &write_options)
            .unwrap();

        let mut ipc_record_batch = Vec::new();

        write_message(&mut ipc_record_batch, encoded_batch, &write_options).unwrap();

        let mut values_binary = NewBinary::new(env, ipc_record_batch.len());
        values_binary.copy_from_slice(&ipc_record_batch);

        result.push(values_binary.into());
    }

    let mut ipc_schema = OwnedBinary::new(ipc_schema_bytes.len()).unwrap();
    ipc_schema.as_mut_slice().copy_from_slice(&ipc_schema_bytes);

    Ok((ipc_schema.release(env), result))
}

fn list_of(item_type: DataType) -> DataType {
    DataType::List(Arc::new(Field::new("item", item_type, true)))
}

fn field_data_type(field_type: Atom) -> NifResult<DataType> {
    let data_type = match field_type {
        t if t == string() => DataType::Utf8,
        t if t == integer() => DataType::Int64,
        t if t == float() => DataType::Float64,
        t if t == boolean() => DataType::Boolean,
        t if t == timestamp() => DataType::Timestamp(TimeUnit::Microsecond, Some("+00".into())),
        t if t == string_map() => DataType::Map(Arc::new(key_value_field("entries")), false),
        t if t == integer_array() => list_of(DataType::Int64),
        t if t == float_array() => list_of(DataType::Float64),
        t if t == string_array() || t == json_array() => list_of(DataType::Utf8),
        _ => return Err(Error::BadArg),
    };

    Ok(data_type)
}

fn key_value_field(name: &str) -> Field {
    Field::new(
        name,
        DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
        ])),
        false,
    )
}

fn arrow_error(error: ArrowError) -> Error {
    Error::Term(Box::new(error.to_string()))
}

fn message_error(message: String) -> Error {
    Error::Term(Box::new(message))
}

/// Parses NDJSON rows and normalizes them to the BigQuery column layout:
/// top-level keys have `.` replaced by `_` (BigQuery column names cannot
/// contain `.`), and elements of `json_array_fields` columns are serialized
/// to JSON strings, keeping attribute lists at one level of nesting.
fn normalize_rows(
    ndjson: &[u8],
    json_array_fields: &HashSet<String>,
) -> Result<Vec<Value>, String> {
    ndjson
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .map(|line| match serde_json::from_slice(line) {
            Ok(Value::Object(object)) => Ok(Value::Object(
                object
                    .into_iter()
                    .map(|(key, value)| {
                        let key = key.replace('.', "_");

                        if json_array_fields.contains(&key) {
                            (key, stringify_elements(value))
                        } else {
                            (key, value)
                        }
                    })
                    .collect::<Map<String, Value>>(),
            )),
            Ok(_) => Err("row is not a JSON object".to_string()),
            Err(error) => Err(error.to_string()),
        })
        .collect()
}

fn stringify_elements(value: Value) -> Value {
    match value {
        Value::Array(items) => Value::Array(
            items
                .into_iter()
                .map(|item| match item {
                    Value::String(_) | Value::Null => item,
                    other => Value::String(other.to_string()),
                })
                .collect(),
        ),
        other => other,
    }
}

/// BigQuery REPEATED columns map to non-nullable Arrow lists with
/// non-nullable items. Attribute maps become lists of key/value structs: a
/// map array has the same physical layout (offsets over a struct of
/// key/value children), so only the type changes. Missing lists and maps
/// become empty lists because REPEATED columns cannot be null.
fn to_repeated(column: &ArrayRef) -> Result<ArrayRef, ArrowError> {
    let list = match column.data_type() {
        DataType::Map(_, _) => {
            let (_field, offsets, entries, _nulls, _ordered) = column.as_map().clone().into_parts();
            ListArray::try_new(
                Arc::new(key_value_field("item")),
                offsets,
                Arc::new(entries),
                None,
            )?
        }
        DataType::List(item) => {
            let (_field, offsets, values, _nulls) = column.as_list::<i32>().clone().into_parts();
            ListArray::try_new(Arc::new(list_item_field(item)), offsets, values, None)?
        }
        _ => return Ok(column.clone()),
    };

    Ok(Arc::new(list))
}

/// BigQuery array elements cannot be null. There is deliberately no timestamp
/// array type: BigQuery fails Arrow appends to `ARRAY<TIMESTAMP>` columns with
/// an internal error (with both "+00" and "UTC" timezones), so timestamp lists
/// are written as `:integer_array` Unix microseconds.
fn list_item_field(item: &Field) -> Field {
    item.clone().with_nullable(false)
}

fn to_bigquery_batch(batch: RecordBatch, schema: &SchemaRef) -> Result<RecordBatch, ArrowError> {
    let columns = batch
        .columns()
        .iter()
        .map(to_repeated)
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(schema.clone(), columns)
}

fn bigquery_schema(read_schema: &Schema) -> SchemaRef {
    let fields: Vec<Field> = read_schema
        .fields()
        .iter()
        .map(|field| match field.data_type() {
            DataType::Map(_, _) => Field::new(
                field.name(),
                DataType::List(Arc::new(key_value_field("item"))),
                false,
            ),
            DataType::List(item) => Field::new(
                field.name(),
                DataType::List(Arc::new(list_item_field(item))),
                false,
            ),
            _ => field.as_ref().clone(),
        })
        .collect();

    Arc::new(Schema::new(fields))
}

/// Encodes NDJSON rows into Arrow IPC messages against an explicit schema,
/// instead of inferring one from the batch. `fields` is a list of
/// `{name, type, required}` tuples where `type` is one of `:string`,
/// `:integer`, `:float`, `:boolean`, `:timestamp` (Unix microseconds),
/// `:string_map`, or an array type (`:integer_array`, `:float_array`,
/// `:string_array`, `:json_array`). Row keys are matched
/// after replacing `.` with `_`. Returns `{:ok, schema, batches}`, or
/// `{:error, message}` when a row does not fit the schema.
#[rustler::nif(schedule = "DirtyCpu")]
fn get_ipc_bytes_with_schema<'a>(
    env: Env<'a>,
    ndjson: Binary<'a>,
    fields: Vec<(String, Atom, bool)>,
    compression: Atom,
) -> NifResult<(Atom, Binary<'a>, Vec<Binary<'a>>)> {
    let write_options = build_write_options(atom_to_compression(compression)?);

    let json_array_fields: HashSet<String> = fields
        .iter()
        .filter(|(_name, field_type, _required)| *field_type == json_array())
        .map(|(name, _field_type, _required)| name.clone())
        .collect();

    let read_fields = fields
        .into_iter()
        .map(|(name, field_type, required)| {
            Ok(Field::new(name, field_data_type(field_type)?, !required))
        })
        .collect::<NifResult<Vec<_>>>()?;

    let read_schema = Arc::new(Schema::new(read_fields));
    let write_schema = bigquery_schema(&read_schema);

    let ipc_schema_bytes = {
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let schema_data = IpcDataGenerator::default().schema_to_bytes_with_dictionary_tracker(
            &write_schema,
            &mut dictionary_tracker,
            &write_options,
        );

        let mut bytes = Vec::new();
        write_message(&mut bytes, schema_data, &write_options).map_err(arrow_error)?;
        bytes
    };

    let rows = normalize_rows(ndjson.as_slice(), &json_array_fields).map_err(message_error)?;

    // the decoder preallocates from the batch size, so size it to the row count
    // to get a single record batch without over-allocating
    let mut decoder = ReaderBuilder::new(read_schema)
        .with_batch_size(rows.len().max(1))
        .build_decoder()
        .map_err(arrow_error)?;

    decoder.serialize(&rows).map_err(arrow_error)?;

    let mut result = Vec::new();

    if let Some(batch) = decoder.flush().map_err(arrow_error)? {
        let batch = to_bigquery_batch(batch, &write_schema).map_err(arrow_error)?;

        let mut dictionary_tracker = DictionaryTracker::new(false);
        let (_, encoded_batch) = IpcDataGenerator::default()
            .encoded_batch(&batch, &mut dictionary_tracker, &write_options)
            .map_err(arrow_error)?;

        let mut ipc_record_batch = Vec::new();
        write_message(&mut ipc_record_batch, encoded_batch, &write_options).map_err(arrow_error)?;

        let mut values_binary = NewBinary::new(env, ipc_record_batch.len());
        values_binary.copy_from_slice(&ipc_record_batch);
        result.push(values_binary.into());
    }

    let mut ipc_schema = OwnedBinary::new(ipc_schema_bytes.len()).ok_or(Error::BadArg)?;
    ipc_schema.as_mut_slice().copy_from_slice(&ipc_schema_bytes);

    Ok((rustler::types::atom::ok(), ipc_schema.release(env), result))
}

rustler::init!("Elixir.Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC");
