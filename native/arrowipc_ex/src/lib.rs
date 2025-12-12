use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Field, Schema, TimeUnit},
    ipc::writer::{write_message, DictionaryTracker, IpcDataGenerator, IpcWriteOptions},
    json::{reader::infer_json_schema, ReaderBuilder},
};
use rustler::{Binary, Env, NewBinary, NifResult, OwnedBinary};

#[rustler::nif(schedule = "DirtyIo")]
fn get_ipc_bytes(env: Env, dataframe_json: String) -> NifResult<(Binary, Vec<Binary>)> {
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

    let ipc_schema_bytes = {
        let mut dictionary_tracker = DictionaryTracker::new(false);
        let data_gen = IpcDataGenerator::default();
        let schema_data = data_gen.schema_to_bytes_with_dictionary_tracker(
            &inferred_schema,
            &mut dictionary_tracker,
            &IpcWriteOptions::default(),
        );

        let mut ipc_schema_bytes = Vec::new();

        write_message(
            &mut ipc_schema_bytes,
            schema_data,
            &IpcWriteOptions::default(),
        )
        .unwrap();

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
            .encoded_batch(
                &batch_record,
                &mut dictionary_tracker,
                &IpcWriteOptions::default(),
            )
            .unwrap();

        let mut ipc_record_batch = Vec::new();

        write_message(
            &mut ipc_record_batch,
            encoded_batch,
            &IpcWriteOptions::default(),
        )
        .unwrap();

        let mut values_binary = NewBinary::new(env, ipc_record_batch.len());
        values_binary.copy_from_slice(&ipc_record_batch);

        result.push(values_binary.into());
    }

    let mut ipc_schema = OwnedBinary::new(ipc_schema_bytes.len()).unwrap();
    ipc_schema.as_mut_slice().copy_from_slice(&ipc_schema_bytes);

    Ok((ipc_schema.release(env), result))
}

rustler::init!("Elixir.Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC");
