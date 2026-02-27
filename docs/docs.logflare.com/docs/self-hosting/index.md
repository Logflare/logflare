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

| Env Var                                        | Type                                                                    | Description                                                                                                                                                                                                                                                          |
| ---------------------------------------------- | ----------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `LOGFLARE_DB_ENCRYPTION_KEY`                   | Base64 encryption key, **required**                                     | Encryption key used for encrypting sensitive data.                                                                                                                                                                                                                   |
| `LOGFLARE_DB_ENCRYPTION_KEY_RETIRED`           | Base64 encryption key, defaults to `nil`                                | The deprecated encryption key to migrate existing database secrets from. Data will be migrated to the key set under `LOGFLARE_DB_ENCRYPTION_KEY`. Used for encryption key rolling only.                                                                              |
| `LOGFLARE_SINGLE_TENANT`                       | Boolean, defaults to `false`                                            | If enabled, a singular user will be seeded. All browser usage will default to the user.                                                                                                                                                                              |
| `LOGFLARE_API_KEY`                             | string, defaults to `nil`                                               | **Deprecated**. Use `LOGFLARE_PUBLIC_ACCESS_TOKEN` instead. Alias for `LOGFLARE_PUBLIC_ACCESS_TOKEN`.                                                                                                                                                                |
| `LOGFLARE_PUBLIC_ACCESS_TOKEN`                 | string, defaults to `nil`                                               | If set, creates a public access token for the provisioned user, for ingestion or querying usage. Single-tenant mode only.                                                                                                                                            |
| `LOGFLARE_PRIVATE_ACCESS_TOKEN`                | string, defaults to `nil`                                               | If set, creates a private access token for the provisioned user, for management API usage. Single-tenant mode only.                                                                                                                                                  |
| `LOGFLARE_SUPABASE_MODE`                       | Boolean, defaults to `false`                                            | A special mode for Logflare, where Supabase-specific resources will be seeded. Intended for Suapbase self-hosted usage.                                                                                                                                              |
| `PHX_HTTP_IP`                                  | String, defaults to `nil`                                               | Allows configuration of the HTTP server IP to bind to. Specifying an IPv6 like `::` will enable IPv6.                                                                                                                                                                |
| `PHX_HTTP_PORT`                                | Integer, defaults to `4000`                                             | Allows configuration of the HTTP server port.                                                                                                                                                                                                                        |
| `DB_SCHEMA`                                    | String, defaults to `nil`                                               | Allows configuration of the database schema to scope Logflare operations.                                                                                                                                                                                            |
| `DB_SSL`                                       | Boolean, defaults to `false`                                            | Enables SSL/TLS connection to the internal Logflare database. Requires certificate files when enabled. See [Database SSL Configuration](#database-ssl-configuration).                                                                                                |
| `LOGFLARE_LOG_LEVEL`                           | String, defaults to `info`. <br/>Options: `error`,`warning`, `info`     | Allows runtime configuration of log level.                                                                                                                                                                                                                           |
| `LOGFLARE_NODE_HOST`                           | string, defaults to `127.0.0.1`                                         | Sets node host on startup, which affects the node name `logflare@<host>`                                                                                                                                                                                             |
| `LOGFLARE_METADATA_CLUSTER`                    | string, defaults to `nil`                                               | Sets global logging/tracing metadata for the cluster name and affects the release node name (e.g., `logflare-production@<host>`). Useful for filtering logs by cluster name and distinguishing nodes in multi-cluster setups. See the [metadata](#Metadata) section. |
| `LOGFLARE_PUBSUB_POOL_SIZE`                    | Integer, defaults to `10`                                               | Sets the number of `Phoenix.PubSub.PG2` partitions to be created. Should be configured to the number of cores of your server for optimal multi-node performance.                                                                                                     |
| `LOGFLARE_ALERTS_ENABLED`                      | Boolean, defaults to `true`                                             | Flag for enabling and disabling query alerts.                                                                                                                                                                                                                        |
| `LOGFLARE_MIN_CLUSTER_SIZE`                    | Integer, defaults to `1`                                                | Sets the target cluster size, and emits a warning log periodically if the cluster is below the set number of nodes..                                                                                                                                                 |
| `LOGFLARE_OTEL_ENDPOINT`                       | String, defaults to `nil`                                               | Sets the OpenTelemetry Endpoint to send traces to via gRPC. Port number can be included, such as `https://logflare.app:443`                                                                                                                                          |
| `LOGFLARE_OTEL_SOURCE_UUID`                    | String, defaults to `nil`, optionally required for OpenTelemetry.       | Sets the appropriate header for ingesting OpenTelemetry events into a Logflare source.                                                                                                                                                                               |
| `LOGFLARE_OTEL_ACCESS_TOKEN`                   | String, defaults to `nil`, optionally required for OpenTelemetry.       | Sets the appropriate authentication header for ingesting OpenTelemetry events into a Logflare source.                                                                                                                                                                |
| `LOGFLARE_OTEL_SAMPLE_RATIO`                   | Float, defaults to `1.0`.                                               | Sets the sample ratio for server traces.                                                                                                                                                                                                                             |
| `LOGFLARE_OTEL_INGEST_SAMPLE_RATIO`            | Float, defaults to the value of `LOGFLARE_OTEL_SAMPLE_RATIO`, optional. | Sets the sample ratio for ingestion-related server traces.                                                                                                                                                                                                           |
| `LOGFLARE_OTEL_ENDPOINT_SAMPLE_RATIO`          | Float, defaults to the value of `LOGFLARE_OTEL_SAMPLE_RATIO`, optional. | Sets the sample ratio for endpoint-related server traces.                                                                                                                                                                                                            |
| `LOGFLARE_HEALTH_MAX_MEMORY_UTILIZATION_RATIO` | Float, defaults to `0.80`                                               | Sets the maximum allowable memory utilization ratio for health checks. If exceeded, the health check will fail.                                                                                                                                                      |
| `LOGFLARE_HTTP_CONNECTION_POOLS`               | String, defaults to `nil`, optionally used for performance tuning.      | Controls which HTTP connection pools are created for backend adaptors. Accepts comma-separated list of providers: `all`, `datadog`. Use `all` for all pools, `datadog` for DataDog only, or omit to use default behavior.                                            |

Additional environment variable configurations for the OpenTelemetry libraries used can be found [here](https://hexdocs.pm/opentelemetry_exporter/readme.html).perf/bq-pipeline-sharding

#### Health Checks

Logflare has a health check endpoint `/health`, which is used to ensure that the system is functioning correctly with sufficient resources for normal functions.

Environment variables that influence the logic are prefixed with `LOGFLARE_HEALTH_*`. Refer to above table for customizing the values.

#### Metadata

Metadata allows passing in configuration values that will be merged into the logs and traces.

Environment variables that influence the logic are prefixed with `LOGFLARE_METADATA_*`. Refer to above table for customizing the values.

Setting `LOGFLARE_METADATA_CLUSTER=production` will result the following payloads:

```json
// for logs
{
  "metadata": {
    "cluster": "production",
    ...
  }
}

// for OpenTelemetry Traces
{
  "attributes": {
    "system.cluster": "production",
    ...
  }
}
```

:::warning
`LOGFLARE_LOGGER_METADATA_CLUSTER` has been renamed to `LOGFLARE_METADATA_CLUSTER` as of `v1.13.x`.
:::

The `LOGFLARE_METADATA_CLUSTER` environment variable is also set in the Erlang node name when set (e.g., `logflare-production@hostname` instead of `logflare@hostname`).

### BigQuery Backend Configuration

| Env Var                             | Type                        | Description                                                                                                                                                                                         |
| ----------------------------------- | --------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `GOOGLE_PROJECT_ID`                 | string, required            | Specifies the GCP project to use.                                                                                                                                                                   |
| `GOOGLE_PROJECT_NUMBER`             | string, required            | Specifies the GCP project to use.                                                                                                                                                                   |
| `GOOGLE_DATASET_ID_APPEND`          | string, defaults to `_prod` | This allows customization of the dataset created in BigQuery.                                                                                                                                       |
| `LOGFLARE_BIGQUERY_MANAGED_SA_POOL` | Integer, defaults to `0`    | Sets the number of managed service accounts to create for BigQuery API operations. When set to 0, managed service accounts are disabled, and all queries will run throguh the main service account. |

#### Managed Service Accounts

When `LOGFLARE_BIGQUERY_MANAGED_SA_POOL` is a non-zero value, managed service accounts will use impersonation when making requests against the BigQuery REST API. Increase this value when experiencing rate limiting.

This is due to BigQuery having a fixed 100 requests per second per user limit on their core REST API. However, service account impersonation allows us to spread out requests across multiple service accounts, thereby avoiding this limitation.

Managed service accounts will be provisioned automatically by the server, hence it will require additional permissions:

- `roles/resourcemanager.projectIamAdmin`
- `roles/iam.serviceAccountCreator`
- `roles/iam.serviceAccountTokenCreator`

Without these two additional permissions, the managed service accounts feature will not work.

### PostgreSQL Backend Configuration

| Env Var                   | Type                                   | Description                                                                                                              |
| ------------------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------ |
| `POSTGRES_BACKEND_URL`    | string, required                       | PostgreSQL connection string, for connecting to the database. User must have sufficient permssions to manage the schema. |
| `POSTGRES_BACKEND_SCHEMA` | string, optional, defaults to `public` | Specifies the database schema to scope all operations.                                                                   |

## Database SSL Configuration

Logflare supports secure SSL/TLS connections to its internal database (not the PostgreSQL backend). This is configured using the `DB_SSL` environment variable and certificate files.

### Requirements

To enable SSL for the internal Logflare database:

1. Set `DB_SSL=true` environment variable
2. Provide three certificate files in the working directory on server startup:
   - `db-server-ca.pem` - Server CA certificate
   - `db-client-cert.pem` - Client certificate
   - `db-client-key.pem` - Client private key

All three files must be present for SSL to be enabled.

### Configuration Details

The SSL connection is configured with:

- **Peer verification**: Enabled (`verify: :verify_peer`)
- **TLS version**: TLS 1.2
- **Wildcard support**: Enabled via `public_key.pkix_verify_hostname_match_fun(:https)`

The configuration follows the [Erlang Security Working Group recommendations](https://erlef.github.io/security-wg/secure_coding_and_deployment_hardening/ssl).

## Database Encryption

Certain database columns that store sensitive data are encrypted with the `LOGFLARE_DB_ENCRYPTION_KEY` key.
Encryption keys must be Base64 encoded.

Cipher used is AES with a 256-bit key in GCM mode.

If `LOGFLARE_DB_ENCRYPTION_KEY` environement variable is not provided, a default hardcoded encryption key will be used.

### Rolling Encryption Keys

In order to roll encryption keys and migrate existing encrypted data, use the `LOGFLARE_DB_ENCRYPTION_KEY_RETIRED` environment variable.

Steps to perform the migration are:

1. Move the retired encryption key from `LOGFLARE_DB_ENCRYPTION_KEY` to `LOGFLARE_DB_ENCRYPTION_KEY_RETIRED`.
2. Generate a new encryption key and set it to `LOGFLARE_DB_ENCRYPTION_KEY`.
3. Restart or deploy the server with the new environment variables.
4. Upon successful server startup, an `info` log will be emitted that says that an retired encryption key is detected, and the migration will be initiated to transition all data encrypted with the retired key to be encrypted with the new key.
5. Once the migration is complete, the retired encryption key can be safely removed. There will be an `info` log that will be emitted once the migration is complete.

## BigQuery Setup

### Pre-requisites

You will need a Google Cloud project **with billing enabled** in order to proceed.

The requirements for server startup are as follows after creating the project:

- Project ID
- Project number
- A service account key

#### Setting up BigQuery Service Account

To ensure that you have sufficient permissions to insert into your Google Cloud BigQuery, ensure that you have created a service account with either:

- `roles/bigquery.admin`
- for [managed service accounts](#managed-service-accounts)
  - `roles/resourcemanager.projectIamAdmin`
  - `role/iam.serviceAccountCreator`
  - `role/iam.serviceAccountTokenCreator`

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
      - DB_SSL=true # Optional: enable SSL for internal database
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
      # Optional: SSL certificate files for internal database
      - type: bind
        source: ${PWD}/db-server-ca.pem
        target: /opt/app/rel/logflare/bin/db-server-ca.pem
        read_only: true
      - type: bind
        source: ${PWD}/db-client-cert.pem
        target: /opt/app/rel/logflare/bin/db-client-cert.pem
        read_only: true
      - type: bind
        source: ${PWD}/db-client-key.pem
        target: /opt/app/rel/logflare/bin/db-client-key.pem
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

## OpenTelemetry

To deploy OpenTelemetry to Cloudflare + GCP successfully, the following need to be accomplished:

1. Expose instance group named port 50051 (or whatever it is set on the server.)
2. Set load balancer to send `otel.logflare.app:443` to the above named port on the designated backend. Backend must be set to HTTP/2 protocol.
3. Between instances and load balancer, connections need to be TLS encrypted. Can be self-signed. Generate self-signed certs with `make ssl.prod` and `make ssl.staging`. No CA cert needs to be used when self-signing, can use public certs.
4. Between CF and GCP, connections should have Full SSL (optionally strict). Generate an origin cert on CF and use it on the load balancer. SSL is terminated at the load balancer.
5. Connecting client should not need an SSL cert to connect (unless you want to encrypt the traffic between client and instance)

### OTel Troubleshooting

1. I'm getting GRPC status 14 with `upstream connect error` with an TLS wrong version number error.

   ```
   upstream connect error or disconnect/reset before headers. reset reason: remote connection failure, transport failure reason: TLS_error:|268435703:SSL routines:OPENSSL_internal:WRONG_VERSION_NUMBER:TLS_error_end
   ```

   It is due fauly or non-existent self-signed certificate on the instance. Regenerate the self-signed certificate.

2. I'm getting gRPC status 14 with a timeout error. Initial request works but streaming errors out.

   Backend protocol must be set to HTTP/2 and self-signed certs must be put on the instance.
