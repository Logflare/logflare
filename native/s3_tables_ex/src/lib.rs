use std::collections::HashMap;
use std::sync::OnceLock;

use iceberg::{Catalog, CatalogBuilder, Namespace, NamespaceIdent};
use iceberg_catalog_s3tables::{S3TablesCatalog, S3TablesCatalogBuilder};
use rustler::{Atom as NifAtom, Encoder, Env, NifMap, Resource, ResourceArc, Term};
use tokio::runtime::Runtime;

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

/// Handle to a constructed S3 Tables catalog client, held across NIF calls.
pub struct CatalogResource {
    #[allow(dead_code)]
    catalog: S3TablesCatalog,
    #[allow(dead_code)]
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

/// Stub: real table creation/validation lands in a later phase.
#[rustler::nif(schedule = "DirtyIo")]
fn ensure_table(
    _catalog: ResourceArc<CatalogResource>,
    _table_name: String,
    _schema_json: String,
) -> NifAtom {
    atoms::ok()
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
