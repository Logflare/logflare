use std::sync::Arc;

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

fn row_count(ndjson: &[u8]) -> usize {
    ndjson
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.iter().all(u8::is_ascii_whitespace))
        .count()
}

/// Splits NDJSON at row boundaries into chunks of at most `max_bytes`, so each
/// chunk becomes one record batch that fits an AppendRows request. A single row
/// longer than `max_bytes` becomes a chunk of its own.
fn byte_bounded_chunks(ndjson: &[u8], max_bytes: usize) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    let mut chunk_start = 0;
    let mut chunk_end = 0;

    while chunk_end < ndjson.len() {
        let row_end = ndjson[chunk_end..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(ndjson.len(), |newline| chunk_end + newline + 1);

        if row_end - chunk_start > max_bytes && chunk_end > chunk_start {
            chunks.push(&ndjson[chunk_start..chunk_end]);
            chunk_start = chunk_end;
        }

        chunk_end = row_end;
    }

    if chunk_end > chunk_start {
        chunks.push(&ndjson[chunk_start..chunk_end]);
    }

    chunks
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

/// Encodes NDJSON rows against an explicit schema, one record batch per
/// `byte_bounded_chunks` chunk. Input contract and field types are documented on
/// `Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC.get_ipc_bytes_with_schema/4`.
#[rustler::nif(schedule = "DirtyCpu")]
fn get_ipc_bytes_with_schema<'a>(
    env: Env<'a>,
    ndjson: Binary<'a>,
    fields: Vec<(String, Atom, bool)>,
    compression: Atom,
    max_batch_bytes: usize,
) -> NifResult<(Atom, Binary<'a>, Vec<Binary<'a>>)> {
    if max_batch_bytes == 0 {
        return Err(Error::BadArg);
    }

    let write_options = build_write_options(atom_to_compression(compression)?);

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

    let mut result = Vec::new();

    for chunk in byte_bounded_chunks(ndjson.as_slice(), max_batch_bytes) {
        // the decoder preallocates from the batch size, so size it to the chunk's
        // row count to get one record batch per chunk without over-allocating
        let mut decoder = ReaderBuilder::new(read_schema.clone())
            .with_batch_size(row_count(chunk).max(1))
            .build_decoder()
            .map_err(arrow_error)?;

        let mut remaining = chunk;
        while !remaining.is_empty() {
            let consumed = decoder.decode(remaining).map_err(arrow_error)?;
            if consumed == 0 {
                break;
            }
            remaining = &remaining[consumed..];
        }

        let Some(batch) = decoder.flush().map_err(arrow_error)? else {
            continue;
        };

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

#[cfg(test)]
mod tests {
    use super::byte_bounded_chunks;

    #[test]
    fn keeps_input_within_the_limit_in_one_chunk() {
        assert_eq!(byte_bounded_chunks(b"ab\ncd\n", 6), vec![&b"ab\ncd\n"[..]]);
    }

    #[test]
    fn splits_at_row_boundaries_when_the_limit_is_exceeded() {
        assert_eq!(
            byte_bounded_chunks(b"ab\ncd\nef\n", 6),
            vec![&b"ab\ncd\n"[..], &b"ef\n"[..]]
        );
    }

    #[test]
    fn gives_a_row_longer_than_the_limit_its_own_chunk() {
        assert_eq!(
            byte_bounded_chunks(b"a\nlong-row\nb\n", 4),
            vec![&b"a\n"[..], &b"long-row\n"[..], &b"b\n"[..]]
        );
    }

    #[test]
    fn keeps_a_final_row_without_a_trailing_newline() {
        assert_eq!(
            byte_bounded_chunks(b"ab\ncd", 3),
            vec![&b"ab\n"[..], &b"cd"[..]]
        );
    }

    #[test]
    fn returns_no_chunks_for_empty_input() {
        assert!(byte_bounded_chunks(b"", 10).is_empty());
    }

    #[test]
    fn keeps_every_byte_in_order() {
        let input = b"one\ntwo\nthree\nfour\nfive\n";

        for max_bytes in 1..=input.len() + 1 {
            let joined: Vec<u8> = byte_bounded_chunks(input, max_bytes).concat();
            assert_eq!(joined, input, "max_bytes = {max_bytes}");
        }
    }
}
