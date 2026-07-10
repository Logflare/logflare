use std::collections::HashMap;
use std::sync::OnceLock;

use iceberg::spec::Transform;
use iceberg::spec::UnboundPartitionSpec;
use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent, TableCreation, TableIdent};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogBuilder};
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

// TODO: Ensure DirtyIO scheduler won't be a bottle neck
/// Constructs a real S3 Tables catalog handle via the AWS credential chain.
#[rustler::nif(schedule = "DirtyIo")]
fn init_catalog<'a>(env: Env<'a>, config: Config) -> Term<'a> {
    let mut props = HashMap::new();

    if !config.access_key_id.is_empty() && !config.secret_access_key.is_empty() {
        props.insert("aws_access_key_id".to_string(), config.access_key_id);
        props.insert(
            "aws_secret_access_key".to_string(),
            config.secret_access_key,
        );
    }

    let result: Result<CatalogResource, iceberg::Error> = runtime().block_on(async {
        let catalog = S3TablesCatalogBuilder::default()
            .with_table_bucket_arn(config.table_bucket_arn)
            .load("s3_tables", props)
            .await?;

        let namespace = catalog
            .get_namespace(&NamespaceIdent::new(config.namespace))
            .await?;

        // let tables = catalog.list_tables(namespace.name()).await?;
        return Ok(CatalogResource { catalog, namespace });
    });

    // the lib does a poor job with errors, everything is flattened to Unexpected with a message
    // TODO: Match message and try to translate to meaningful errors
    match result {
        Ok(resource) => (atoms::ok(), ResourceArc::new(resource)).encode(env),
        Err(err) => (atoms::error(), format!("{err:?}")).encode(env),
    }
}

/// Creates the Iceberg table for `table_name` from the given field list if it doesn't already
/// exist. Idempotent: returns `{:ok, :already_exists}` both when the table was already present
/// and when AWS reports a conflict from a concurrent create (two sources provisioning the same
/// backend at once).
#[rustler::nif(schedule = "DirtyIo")]
fn ensure_table<'a>(
    env: Env<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
    fields: Vec<FieldSpec>,
) -> Term<'a> {
    let result: Result<NifAtom, String> = runtime().block_on(async {
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
    });

    match result {
        Ok(atom) => (atoms::ok(), atom).encode(env),
        Err(err) => (atoms::error(), err).encode(env),
    }
}

/// Returns the current column names of an existing Iceberg table, used to confirm table creation.
#[rustler::nif(schedule = "DirtyIo")]
fn table_columns<'a>(
    env: Env<'a>,
    catalog: ResourceArc<CatalogResource>,
    table_name: String,
) -> Term<'a> {
    let result: Result<Vec<String>, String> = runtime().block_on(async {
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
            .collect();

        Ok(names)
    });

    match result {
        Ok(names) => (atoms::ok(), names).encode(env),
        Err(err) => (atoms::error(), err).encode(env),
    }
}

/// Stub: real Arrow IPC batch append lands in a later phase.
#[rustler::nif(schedule = "DirtyIo")]
fn append_batch(_catalog: ResourceArc<CatalogResource>, _arrow_ipc: rustler::Binary) -> NifAtom {
    atoms::ok()
}

fn on_load(env: Env, _info: Term) -> bool {
    env.register::<CatalogResource>().is_ok()
}

rustler::init!(
    "Elixir.Logflare.Backends.Adaptor.S3TablesAdaptor.Native",
    load = on_load
);
