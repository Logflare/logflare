use std::collections::HashMap;
use std::sync::OnceLock;

use iceberg::spec::Transform;
use iceberg::spec::UnboundPartitionSpec;
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogBuilder};
use rustler::OwnedEnv;
use rustler::{Atom as NifAtom, Encoder, Env, NifMap, Resource, ResourceArc, Term};
use tokio::runtime::Runtime;

mod schema;

use schema::FieldSpec;

mod atoms {
    rustler::atoms! {
        ok,
        error,
        created,
        already_exists,
    }
}

const TIMESTAMP_PARTITION_NAME: &str = "timestamp_day";

/// Handle to a constructed S3 Tables catalog client, held across NIF calls.
pub struct CatalogResource {
    catalog: S3TablesCatalog,
    namespace: Namespace,
}

impl Resource for CatalogResource {}

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
fn init_catalog_nif<'a>(env: Env<'a>, result_tag: Term<'a>, config: Config) -> NifAtom {
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

            Ok(CatalogResource { catalog, namespace })
        }
        .await
        // the lib does a poor job with errors, everything is flattened to Unexpected with a message
        // TODO: Match message and try to translate to meaningful errors
        .map_err(|err: iceberg::Error| format!("{err:?}"))
        .map(ResourceArc::new)
    })
}

/// Creates the Iceberg table for `table_name` from the given field list if it doesn't already
/// exist, stamping `properties` (e.g. `logflare.schema-version`) into the table metadata.
/// Idempotent: returns `{:ok, :already_exists}` both when the table was already present
/// and when AWS reports a conflict from a concurrent create.
#[rustler::nif]
fn ensure_table_nif<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
    fields: Vec<FieldSpec>,
    properties: HashMap<String, String>,
) -> NifAtom {
    spawn_reply(env, result_tag, async move {
        let table_ident = TableIdent::new(catalog.namespace.name().clone(), table_name.clone());

        match catalog.catalog.table_exists(&table_ident).await {
            Ok(true) => return Ok(atoms::already_exists()),
            Ok(false) => {}
            Err(err) => return Err(format!("{err:?}")),
        }

        let (table_schema, timestamp_field_id) = schema::build(&fields)?;

        let partition_spec = UnboundPartitionSpec::builder()
            .add_partition_field(timestamp_field_id, TIMESTAMP_PARTITION_NAME, Transform::Day)
            .map_err(|err| format!("{err:?}"))?
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
                let message = format!("{err:?}");
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

/// Returns the current column names of an existing Iceberg table, used to confirm table creation.
#[rustler::nif]
fn table_columns_nif<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
) -> NifAtom {
    spawn_reply(env, result_tag, async move {
        let table_ident = TableIdent::new(catalog.namespace.name().clone(), table_name);

        let table = catalog
            .catalog
            .load_table(&table_ident)
            .await
            .map_err(|err| format!("{err:?}"))?;

        let names = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|field| field.name.clone())
            .collect::<Vec<String>>();

        Ok::<_, String>(names)
    })
}

/// Stub: real Arrow IPC batch append lands in a later phase.
#[rustler::nif]
fn append_batch_nif<'a>(
    env: Env<'a>,
    result_tag: Term<'a>,
    _catalog: ResourceArc<CatalogResource>,
    _arrow_ipc: rustler::Binary,
) -> NifAtom {
    spawn_reply(env, result_tag, async move { atoms::ok() })
}

fn on_load(env: Env, _info: Term) -> bool {
    env.register::<CatalogResource>().is_ok()
}

rustler::init!(
    "Elixir.Logflare.Backends.Adaptor.S3TablesAdaptor.Native",
    load = on_load
);
