# Self Hosting

Logflare can be self-hosted. As of now, only a single machine setup is supported.

Two different backends are supported:

- BigQuery
- PostgreSQL (experimental)

Docker-compose is the recommended way to manage single node deployments.

### Limitations

Inviting team users and other team-related functionality is currently not supported, as Logflare self-hosted is currently intended for single-user experience only.

All browser authentication will be disabled when in single-tenant mode.

## Configuration

### Common Configuration

| Env Var                                | Type                                                                | Description                                                                                                                                                                             |
| -------------------------------------- | ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LOGFLARE_DB_ENCRYPTION_KEY`           | Base64 encryption key, **required**                                 | Encryption key used for encrypting sensitive data.                                                                                                                                      |
| `LOGFLARE_DB_ENCRYPTION_KEY_OLD`       | Base64 encryption key, defaults to `nil`                            | The deprecated encryption key to migrate existing database secrets from. Data will be migrated to the key set under `LOGFLARE_DB_ENCRYPTION_KEY`. Used for encryption key rolling only. |
| `LOGFLARE_SINGLE_TENANT`               | Boolean, defaults to `false`                                        | If enabled, a singular user will be seeded. All browser usage will default to the user.                                                                                                 |
| `LOGFLARE_API_KEY`                     | string, defaults to `nil`                                           | If set, this API Key can be used for interacting with the Logflare API. API key will be automatically generated if not set.                                                             |
| `LOGFLARE_SUPABASE_MODE`               | Boolean, defaults to `false`                                        | A special mode for Logflare, where Supabase-specific resources will be seeded. Intended for Suapbase self-hosted usage.                                                                 |
| `PHX_HTTP_PORT`                        | Integer, defaults to `4000`                                         | Allows configuration of the HTTP server port.                                                                                                                                           |
| `DB_SCHEMA`                            | String, defaults to `nil`                                           | Allows configuration of the database schema to scope Logflare operations.                                                                                                               |
| `LOGFLARE_LOG_LEVEL`                   | String, defaults to `info`. <br/>Options: `error`,`warning`, `info` | Allows runtime configuration of log level.                                                                                                                                              |
| `LOGFLARE_NODE_HOST`                   | string, defaults to `127.0.0.1`                                     | Sets node host on startup, which affects the node name `logflare@<host>`                                                                                                                |
| `LOGFLARE_LOGGER_METADATA_CLUSTER`     | string, defaults to `nil`                                           | Sets global logging metadata for the cluster name. Useful for filtering logs by cluster name.                                                                                           |
| `LOGFLARE_PUBSUB_POOL_SIZE`            | Integer, defaults to `10`                                           | Sets the number of `Phoenix.PubSub.PG2` partitions to be created. Should be configured to the number of cores of your server for optimal multi-node performance.                        |
| `LOGFLARE_ALERTS_ENABLED`              | Boolean, defaults to `true`                                         | Flag for enabling and disabling query alerts.                                                                                                                                           |
| `LOGFLARE_ALERTS_MIN_CLUSTER_SIZE`     | Integer, defaults to `1`                                            | Sets the required cluster size for Query Alerts to be run. If cluster size is below the provided value, query alerts will not run.                                                      |
| `LOGFLARE_MIN_CLUSTER_SIZE`            | Integer, defaults to `1`                                            | Sets the target cluster size, and emits a warning log periodically if the cluster is below the set number of nodes..                                                                    |
| `LOGFLARE_OTEL_ENDPOINT`               | String, defaults to `nil`                                           | Sets the OpenTelemetry Endpoint to send traces to via gRPC. Port number can be included, such as `https://logflare.app:443`                                                             |
| `LOGFLARE_OTEL_SOURCE_UUID`            | String, defaults to `nil`, optionally required for OpenTelemetry.   | Sets the appropriate header for ingesting OpenTelemetry events into a Logflare source.                                                                                                  |
| `LOGFLARE_OTEL_ACCESS_TOKEN`           | String, defaults to `nil`, optionally required for OpenTelemetry.   | Sets the appropriate authentication header for ingesting OpenTelemetry events into a Logflare source.                                                                                   |
| `LOGFLARE_OPEN_TELEMETRY_SAMPLE_RATIO` | Float, defaults to `0.001`, optionally required for OpenTelemetry.  | Sets the sample ratio for server traces. Ingestion and Endpoint routes are dropped and are not included in tracing.                                                                     |

LOGFLARE_OPEN_TELEMETRY_SAMPLE_RATIO
Additional environment variable configurations for the OpenTelemetry libraries used can be found [here](https://hexdocs.pm/opentelemetry_exporter/readme.html).perf/bq-pipeline-sharding

### BigQuery Backend Configuration

| Env Var                    | Type                        | Description                                                   |
| -------------------------- | --------------------------- | ------------------------------------------------------------- |
| `GOOGLE_PROJECT_ID`        | string, required            | Specifies the GCP project to use.                             |
| `GOOGLE_PROJECT_NUMBER`    | string, required            | Specifies the GCP project to use.                             |
| `GOOGLE_DATASET_ID_APPEND` | string, defaults to `_prod` | This allows customization of the dataset created in BigQuery. |

### PostgreSQL Backend Configuration

| Env Var                   | Type                                   | Description                                                                                                              |
| ------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `POSTGRES_BACKEND_URL`    | string, required                       | PostgreSQL connection string, for connecting to the database. User must have sufficient permssions to manage the schema. |
| `POSTGRES_BACKEND_SCHEMA` | string, optional, defaults to `public` | Specifies the database schema to scope all operations.                                                                   |

## Database Encryption

Certain database columns that store sensitive data are encrypted with the `LOGFLARE_DB_ENCRYPTION_KEY` key.
Encryption keys must be Base64 encoded.

Cipher used is AES with a 256-bit key in GCM mode.

### Rolling Encryption Keys

In order to roll encryption keys and migrate existing encrypted data, use the `LOGFLARE_DB_ENCRYPTION_KEY_OLD` environment variable.

Steps to perform the migration are:

1. Move the old encryption key from `LOGFLARE_DB_ENCRYPTION_KEY` to `LOGFLARE_DB_ENCRYPTION_KEY_OLD`.
2. Generate a new encryption key and set it to `LOGFLARE_DB_ENCRYPTION_KEY`.
3. Restart or deploy the server with the new environment variables.
4. Upon successful server startup, an `info` log will be emitted that says that an old encryption key is detected, and the migration will be initiated to transition all data encrypted with the old key to be encrypted with the new key.
5. Once the migration is complete, the old encryption key can be safely removed.

## BigQuery Setup

### Pre-requisites

You will need a Google Cloud project **with billing enabled** in order to proceed.

The requirements for server startup are as follows after creating the project:

- Project ID
- Project number
- A service account key

#### Setting up BigQuery Service Account

To ensure that you have sufficient permissions to insert into your Google Cloud BigQuery, ensure that you have created a service account with either:

- BigQuery Admin role; or
- The following permissions:
  - bigquery.datasets.create
  - bigquery.datasets.get
  - bigquery.datasets.getIamPolicy
  - bigquery.datasets.update
  - bigquery.jobs.create
  - bigquery.routines.create
  - bigquery.routines.update
  - bigquery.tables.create
  - bigquery.tables.delete
  - bigquery.tables.get
  - bigquery.tables.getData
  - bigquery.tables.update
  - bigquery.tables.updateData

We recommend setting the BigQuery Admin role, as it simplifies permissions setup.

#### Obtaining the BigQuery Service Account Key

In order for Logflare to connect sources to their relevant BigQuery tables, we would need to have a service account key that can sign the JWTs needed to authenticate with the Google Cloud APIs.

To obtain the BigQuery service account key after creating it, navigate to IAM > Service Accounts in the web console and click on the "Manage Keys" action option.

![Service Account List Manage Keys Action](./service-account-mange-keys.png)

Thereafter, click on "Add Key" to create a new key. The key will be in a JSON format. Store this key securely on your host machine.

![Add Key Button](add-key.png)

You can also obtain the key via the `gcloud` cli by following the [official documentation](https://cloud.google.com/iam/docs/keys-create-delete).

## Deployment with Docker Compose

Using docker compose is the **recommended method** for self-hosting.

1. Create the `docker-compose.yml`

```yaml
services:
  db:
    image: postgres:13.4-alpine
    environment:
      POSTGRES_PASSWORD: postgres
      POSTGRES_USER: postgres
      POSTGRES_DATABASE: logflare_docker
    ports:
      - "5432:5432"
    volumes:
      - ./priv/wal.sql:/docker-entrypoint-initdb.d/wal.sql
      - pg-data:/var/lib/postgresql/data
  logflare:
    image: supabase/logflare:1.0.1
    ports:
      - "4000:4000"
    hostname: 127.0.0.1
    environment:
      - DB_DATABASE=logflare_docker
      - DB_HOSTNAME=db
      - DB_PORT=5432
      - DB_PASSWORD=postgres
      - DB_USERNAME=postgres
      - LOGFLARE_SINGLE_TENANT=true
      - LOGFLARE_API_KEY=my-cool-api-key

      # Required for BigQuery backend
      - GOOGLE_DATASET_ID_APPEND=_your_env
      - GOOGLE_PROJECT_ID=logflare-docker-example
      - GOOGLE_PROJECT_NUMBER=123123123213

      # Required for Postgres backend
      - POSTGRES_BACKEND_URL=postgresql://user:pass@host:port/db
      - POSTGRES_BACKEND_SCHEMA=my_schema
    volumes:
      - type: bind
        source: ${PWD}/.env
        target: /tmp/.secrets.env
        read_only: true
      - type: bind
        source: ${PWD}/gcloud.json
        target: /opt/app/rel/logflare/bin/gcloud.json
        read_only: true
    depends_on:
      - db
```

2. Using the Service Account key that you had obtained under [the pre-requisites section](#pre-requisites), move and rename the JSON file to `gcloud.json` in your working directory.

The directory structure should be as follows:

```
\
|- gcloud.json
|- docker-compose.yml
```

4. Run `docker-compose up -d` and visit http://localhost:4000

### Using an `.env` file

You can optionally use a `.env` file to manage your environemnts. You can base the file contents on this [reference file](https://github.com/Logflare/logflare/blob/master/.docker.env)

:::note
You cannot have comments in the env file as we load it at startup via `xargs`.
:::

```yaml
# ... the rest is the same
volumes:
  # add in this bind bound. If you have a different name or location, update the source
  - type: bind
    source: ${PWD}/.env
    target: /tmp/.secrets.env
    read_only: true
```

The directory structure will now be as follows:

```
\
|- gcloud.json
|- .env
|- docker-compose.yml
```

## Protecting the User Interface

When self-hosting, it is advised to protect the user interface with minimally basic HTTP authentication. How this is achieved is left to the self-hoster.

When applying such authentication rules, we recommend requiring all routes to be authenticated except for the following paths, as illustrated using glob patterns:

```text
/api/**/*
/logs/**/*
```

the `/logs` path is for legacy reasons and is mostly for compatibility with older Logflare libraries.
