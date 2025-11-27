use std::sync::Arc;

use arrow::{
    ipc::{
        convert::IpcSchemaEncoder,
        writer::{DictionaryTracker, StreamWriter},
    },
    json::{reader::infer_json_schema, ReaderBuilder},
};
use rustler::{Binary, Env, NifResult, OwnedBinary};

#[rustler::nif]
fn get_ipc_bytes(env: Env, dataframe_json: String) -> NifResult<(Binary, Binary)> {
    // convert to Newline delimited JSON (the reader only accepts this format)
    // remove the [] brackets
    let mut json_chars = dataframe_json.chars();
    json_chars.next();
    json_chars.next_back();
    // replace comma separators to new line
    let dataframe_json = json_chars.as_str().replace("},{", "}\n{");
    let mut byte_data = dataframe_json.as_bytes();

    let msg = format!("{dataframe_json}");

    let (inferred_schema, _) = infer_json_schema(&mut byte_data, None).expect(&msg);

    let inferred_schema = Arc::new(inferred_schema);

    let mut dictionary_tracker = DictionaryTracker::new(true);
    let fb = IpcSchemaEncoder::new()
        .with_dictionary_tracker(&mut dictionary_tracker)
        .schema_to_fb(&inferred_schema);
    let ipc_schema_bytes = fb.finished_data();

    let json_reader = ReaderBuilder::new(inferred_schema.clone())
        .build(byte_data)
        .unwrap();

    let mut ipc_record_batch = Vec::new();

    let mut writer = StreamWriter::try_new(&mut ipc_record_batch, &inferred_schema).unwrap();

    for batch_record_result in json_reader {
        if let Ok(batch_record) = batch_record_result {
            writer.write(&batch_record).unwrap();
        }
    }

    writer.finish().unwrap();

    let mut ipc_schema = OwnedBinary::new(ipc_schema_bytes.len()).unwrap();
    ipc_schema.as_mut_slice().copy_from_slice(ipc_schema_bytes);

    let mut ipc_records = OwnedBinary::new(ipc_record_batch.len()).unwrap();
    ipc_records
        .as_mut_slice()
        .copy_from_slice(&ipc_record_batch);

    Ok((ipc_schema.release(env), ipc_records.release(env)))
}

rustler::init!("Elixir.Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC");
