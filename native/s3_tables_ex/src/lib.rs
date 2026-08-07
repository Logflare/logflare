use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_cast::cast;
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, Schema as ArrowSchema, TimeUnit};
use iceberg::arrow::{schema_to_arrow_schema, RecordBatchPartitionSplitter};
use iceberg::spec::{DataFileFormat, Transform, UnboundPartitionSpec};
use iceberg::transaction::{ApplyTransactionAction, Transaction};
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::partitioning::fanout_writer::FanoutWriter;
use iceberg::writer::partitioning::PartitioningWriter;
use iceberg::{
    Catalog, CatalogBuilder, ErrorKind, Namespace, NamespaceIdent, TableCreation, TableIdent,
};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogBuilder};
use parquet::file::properties::WriterProperties;
use rustler::OwnedEnv;
use rustler::{Atom as NifAtom, Encoder, Env, NifMap, Resource, ResourceArc, Term};
use tokio::runtime::Runtime;
use uuid::Uuid;

mod schema;

use schema::FieldSpec;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        created,
        already_exists,
        commit_conflict,
        timeout,
    }
}

/// Upper bound for the entire append (parquet upload + commit retries); kept
/// below the Elixir-side receive timeout so callers get a real error instead
/// of a leaked late message.
const APPEND_TIMEOUT: Duration = Duration::from_secs(55);

/// Rows buffered per decoded `RecordBatch`; above the pipeline's max batch
/// size so a single flush normally yields one batch per NIF call.
const DECODER_BATCH_SIZE: usize = 1 << 16;

const TIMESTAMP_PARTITION_NAME: &str = "timestamp_day";

/// Handle to a constructed S3 Tables catalog client, held across NIF calls.
pub struct CatalogResource {
    catalog: S3TablesCatalog,
    namespace: Namespace,
}

impl Resource for CatalogResource {}

impl CatalogResource {
    fn table_ident(&self, table_name: impl Into<String>) -> TableIdent {
        TableIdent::new(self.namespace.name().clone(), table_name.into())
    }
}

/// Flattens any error into its `{:?}` string. The S3 Tables / iceberg libraries
/// collapse most failures into an opaque `Unexpected` message, so this is the
/// best fidelity available to the Elixir side.
fn fmt_err<E: std::fmt::Debug>(err: E) -> String {
    format!("{err:?}")
}

#[derive(Debug, NifMap)]
struct Config {
    table_bucket_arn: String,
    access_key_id: String,
    secret_access_key: String,
    namespace: String,
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| Runtime::new().expect("failed to start s3_tables_ex tokio runtime"))
}

/// Runs `future` on the shared tokio runtime and, once it resolves, sends
/// `{result_tag, encoded_output}` back to the calling process via an owned
/// env. The NIF call itself returns immediately with `:ok`; callers on the
/// Elixir side receive the real result as a message keyed by `result_tag`.
fn spawn_reply<'a, T, F>(env: Env<'a>, result_tag: Term<'a>, future: F) -> NifAtom
where
    T: Encoder + Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
{
    let pid = env.pid();
    let mut owned_env = OwnedEnv::new();
    let saved_tag = owned_env.save(result_tag);

    runtime().spawn(async move {
        let result = future.await;

        let _ = owned_env.send_and_clear(&pid, |thread_env| {
            let tag = saved_tag.load(thread_env);
            (tag, result).encode(thread_env)
        });
    });

    atoms::ok()
}

/// Constructs a real S3 Tables catalog handle via the AWS credential chain.
#[rustler::nif]
fn init_catalog<'a>(env: Env<'a>, result_tag: Term<'a>, config: Config) -> NifAtom {
    let mut props = HashMap::new();

    if !config.access_key_id.is_empty() && !config.secret_access_key.is_empty() {
        props.insert("aws_access_key_id".to_string(), config.access_key_id);
        props.insert(
            "aws_secret_access_key".to_string(),
            config.secret_access_key,
        );
    }

    spawn_reply(env, result_tag, async move {
        async {
            let catalog = S3TablesCatalogBuilder::default()
                .with_table_bucket_arn(config.table_bucket_arn)
                .load("s3_tables", props)
                .await?;

            let namespace = catalog
                .get_namespace(&NamespaceIdent::new(config.namespace))
                .await?;

            Ok::<_, iceberg::Error>(CatalogResource { catalog, namespace })
        }
        .await
        // the lib does a poor job with errors, everything is flattened to Unexpected with a message
        // TODO: Match message and try to translate to meaningful errors
        .map_err(fmt_err)
        .map(ResourceArc::new)
    })
}

/// Creates the Iceberg table for `table_name` from the given field list if it doesn't already
/// exist, stamping `properties` (e.g. `logflare.schema-version`) into the table metadata.
/// Idempotent: returns `{:ok, :already_exists}` both when the table was already present
/// and when AWS reports a conflict from a concurrent create.
#[rustler::nif]
fn ensure_table<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
    fields: Vec<FieldSpec>,
    properties: HashMap<String, String>,
) -> NifAtom {
    spawn_reply(env, result_tag, async move {
        let table_ident = catalog.table_ident(table_name.clone());

        match catalog.catalog.table_exists(&table_ident).await {
            Ok(true) => return Ok(atoms::already_exists()),
            Ok(false) => {}
            Err(err) => return Err(fmt_err(err)),
        }

        let (table_schema, timestamp_field_id) = schema::build(&fields)?;

        let partition_spec = UnboundPartitionSpec::builder()
            .add_partition_field(timestamp_field_id, TIMESTAMP_PARTITION_NAME, Transform::Day)
            .map_err(fmt_err)?
            .build();

        let creation = TableCreation::builder()
            .name(table_name)
            .schema(table_schema)
            .partition_spec(partition_spec)
            .properties(properties)
            .build();

        match catalog
            .catalog
            .create_table(catalog.namespace.name(), creation)
            .await
        {
            Ok(_) => Ok(atoms::created()),
            Err(err) => {
                let message = fmt_err(err);
                // the lib flattens all errors to Unexpected with a message, so the
                // concurrent-create-conflict race is detected via a lenient string match.
                if message.to_lowercase().contains("conflict")
                    || message.to_lowercase().contains("already exist")
                {
                    Ok(atoms::already_exists())
                } else {
                    Err(message)
                }
            }
        }
    })
}

#[derive(NifMap)]
struct TableInfo {
    columns: Vec<String>,
    properties: HashMap<String, String>,
}

/// Returns the current column names and table properties of an existing
/// Iceberg table, used to confirm table creation and detect schema drift.
#[rustler::nif]
fn table_info<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
) -> NifAtom {
    spawn_reply(env, result_tag, async move {
        let table_ident = catalog.table_ident(table_name);

        let table = catalog
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(fmt_err)?;

        let metadata = table.metadata();

        let columns = metadata
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<String>>();

        Ok::<_, String>(TableInfo {
            columns,
            properties: metadata.properties().clone(),
        })
    })
}

/// Drops an Iceberg table, discarding its data.
#[rustler::nif]
fn drop_table<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
) -> NifAtom {
    spawn_reply(env, result_tag, async move {
        let table_ident = catalog.table_ident(table_name);

        catalog
            .catalog
            .drop_table(&table_ident)
            .await
            .map_err(fmt_err)?;

        Ok::<_, String>(atoms::ok())
    })
}

#[derive(NifMap)]
struct AppendOk {
    row_count: u64,
    data_files: u64,
}

enum AppendError {
    CommitConflict,
    Timeout,
    Other(String),
}

impl From<iceberg::Error> for AppendError {
    fn from(err: iceberg::Error) -> Self {
        AppendError::Other(fmt_err(err))
    }
}

impl From<arrow_schema::ArrowError> for AppendError {
    fn from(err: arrow_schema::ArrowError) -> Self {
        AppendError::Other(fmt_err(err))
    }
}

impl Encoder for AppendError {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            AppendError::CommitConflict => atoms::commit_conflict().encode(env),
            AppendError::Timeout => atoms::timeout().encode(env),
            AppendError::Other(message) => message.encode(env),
        }
    }
}

/// Decodes newline-delimited JSON rows into Arrow record batches, writes them
/// as Iceberg parquet data files (fanned out per day partition), and commits a
/// fast-append transaction. Commit conflicts are retried by the iceberg crate
/// itself (bounded by the `commit.retry.*` table properties); exhaustion
/// surfaces as `{:error, :commit_conflict}`.
///
/// Contract: integer values in `timestamptz` columns are interpreted as unix
/// **nanoseconds** (the unit the mapper emits) and are scaled down to the
/// microseconds Iceberg stores; RFC3339 strings are accepted in any unit.
#[rustler::nif]
fn append_batch<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
    ndjson: rustler::Binary,
) -> NifAtom {
    // the binary term is bound to the calling env, so its bytes must be
    // copied out before crossing into the tokio runtime
    let bytes = ndjson.as_slice().to_vec();

    spawn_reply(env, result_tag, async move {
        match tokio::time::timeout(APPEND_TIMEOUT, do_append(&catalog, table_name, bytes)).await {
            Ok(result) => result,
            Err(_elapsed) => Err(AppendError::Timeout),
        }
    })
}

async fn do_append(
    catalog: &CatalogResource,
    table_name: String,
    ndjson: Vec<u8>,
) -> Result<AppendOk, AppendError> {
    let table_ident = catalog.table_ident(table_name);
    let table = catalog.catalog.load_table(&table_ident).await?;

    let iceberg_schema = table.metadata().current_schema().clone();
    let arrow_schema = Arc::new(schema_to_arrow_schema(&iceberg_schema)?);
    // arrow-json takes raw JSON integers in a timestamp column as already
    // being in the column's unit, but callers send mapper-produced
    // nanoseconds -- so decode against a nanosecond variant of the schema
    // and cast down to the table's microsecond columns afterwards
    let decode_schema = Arc::new(to_nanosecond_schema(&arrow_schema));

    let mut decoder = ReaderBuilder::new(decode_schema)
        .with_batch_size(DECODER_BATCH_SIZE)
        .build_decoder()?;

    let mut batches = Vec::new();
    let mut pos = 0;

    while pos < ndjson.len() {
        let read = decoder.decode(&ndjson[pos..])?;

        if read == 0 {
            break;
        }

        pos += read;

        if let Some(batch) = decoder.flush()? {
            batches.push(cast_batch(&batch, &arrow_schema)?);
        }
    }

    while let Some(batch) = decoder.flush()? {
        batches.push(cast_batch(&batch, &arrow_schema)?);
    }

    let row_count: u64 = batches.iter().map(|batch| batch.num_rows() as u64).sum();

    if row_count == 0 {
        return Ok(AppendOk {
            row_count: 0,
            data_files: 0,
        });
    }

    let splitter = RecordBatchPartitionSplitter::try_new_with_computed_values(
        iceberg_schema.clone(),
        table.metadata().default_partition_spec().clone(),
    )?;

    let parquet_builder = ParquetWriterBuilder::new(WriterProperties::default(), iceberg_schema);
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())?;
    let file_name_generator = DefaultFileNameGenerator::new(
        "part".to_string(),
        Some(Uuid::new_v4().to_string()),
        DataFileFormat::Parquet,
    );
    let rolling_builder = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );

    let mut writer = FanoutWriter::new(DataFileWriterBuilder::new(rolling_builder));

    for batch in &batches {
        for (partition_key, partition_batch) in splitter.split(batch)? {
            writer.write(partition_key, partition_batch).await?;
        }
    }

    let data_files = writer.close().await?;
    let data_file_count = data_files.len() as u64;

    let tx = Transaction::new(&table);
    let tx = tx.fast_append().add_data_files(data_files).apply(tx)?;

    match tx.commit(&catalog.catalog).await {
        Ok(_table) => Ok(AppendOk {
            row_count,
            data_files: data_file_count,
        }),
        Err(err) if err.kind() == ErrorKind::CatalogCommitConflicts => {
            Err(AppendError::CommitConflict)
        }
        Err(err) => Err(err.into()),
    }
}

/// Returns a copy of `schema` with every microsecond timestamp column
/// (including inside lists) widened to nanoseconds, for JSON decoding.
fn to_nanosecond_schema(schema: &ArrowSchema) -> ArrowSchema {
    let fields: Vec<Arc<Field>> = schema
        .fields()
        .iter()
        .map(|field| Arc::new(to_nanosecond_field(field)))
        .collect();

    ArrowSchema::new(fields)
}

fn to_nanosecond_field(field: &Field) -> Field {
    Field::new(
        field.name(),
        to_nanosecond_type(field.data_type()),
        field.is_nullable(),
    )
    .with_metadata(field.metadata().clone())
}

fn to_nanosecond_type(data_type: &DataType) -> DataType {
    match data_type {
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            DataType::Timestamp(TimeUnit::Nanosecond, tz.clone())
        }
        DataType::List(child) => DataType::List(Arc::new(to_nanosecond_field(child))),
        DataType::LargeList(child) => DataType::LargeList(Arc::new(to_nanosecond_field(child))),
        other => other.clone(),
    }
}

/// Casts a batch decoded with the nanosecond schema back to the table's
/// arrow schema, scaling timestamp columns down to microseconds and
/// restoring the Iceberg field-id metadata the writers rely on.
fn cast_batch(
    batch: &RecordBatch,
    target_schema: &Arc<ArrowSchema>,
) -> Result<RecordBatch, AppendError> {
    let columns = batch
        .columns()
        .iter()
        .zip(target_schema.fields())
        .map(|(column, field)| {
            if column.data_type() == field.data_type() {
                Ok(column.clone())
            } else {
                cast(column, field.data_type()).map_err(AppendError::from)
            }
        })
        .collect::<Result<Vec<_>, _>>()?;

    RecordBatch::try_new(target_schema.clone(), columns).map_err(AppendError::from)
}

#[derive(NifMap)]
struct SnapshotInfo {
    snapshot_id: i64,
    operation: String,
    summary: HashMap<String, String>,
    snapshot_count: u64,
}

/// Returns the current snapshot's id and summary (e.g. `added-records`,
/// `total-records`) plus the total snapshot count; `{:ok, nil}` when the
/// table has no snapshots yet.
#[rustler::nif]
fn snapshot_info<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
) -> NifAtom {
    spawn_reply(env, result_tag, async move {
        let table_ident = catalog.table_ident(table_name);

        let table = catalog
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(fmt_err)?;

        let metadata = table.metadata();

        let info = metadata.current_snapshot().map(|snapshot| {
            let summary = snapshot.summary();

            SnapshotInfo {
                snapshot_id: snapshot.snapshot_id(),
                operation: summary.operation.as_str().to_string(),
                summary: summary.additional_properties.clone(),
                snapshot_count: metadata.snapshots().len() as u64,
            }
        });

        Ok::<_, String>(info)
    })
}

fn on_load(env: Env, _info: Term) -> bool {
    env.register::<CatalogResource>().is_ok()
}

rustler::init!(
    "Elixir.Logflare.Backends.Adaptor.S3TablesAdaptor.Native.Nifs",
    load = on_load
);
