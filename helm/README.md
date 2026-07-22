# Logflare Helm Chart

Deploys single-tenant Logflare on Kubernetes, supporting either **BigQuery** or **Postgres** as the event storage backend.

## Quick install

```sh
helm install logflare ./helm -f my-values.yaml
```

At minimum, `my-values.yaml` must pick a backend and supply that backend's credentials (see below).

## Configuring the backend

Set `logflare.backend.type` to `bigquery` or `postgres`. These modes are mutually exclusive.

**BigQuery:**

```yaml
logflare:
  backend:
    type: bigquery
    bigquery:
      projectId: "my-gcp-project"
      projectNumber: "1234567890"
```

The service account key itself is a secret — see [Loading secrets](#loading-secrets).

**Postgres:**

```yaml
logflare:
  backend:
    type: postgres
    postgres:
      schema: "public"
```

The connection URL (`POSTGRES_BACKEND_URL`) is a secret and is not set here — see below.

## Loading secrets

The chart needs several sensitive values: `LOGFLARE_PUBLIC_ACCESS_TOKEN`, `LOGFLARE_PRIVATE_ACCESS_TOKEN`, `DB_PASSWORD`, `LOGFLARE_DB_ENCRYPTION_KEY`, `PHX_SECRET_KEY_BASE`, `PHX_LIVE_VIEW_SIGNING_SALT`, and one of `POSTGRES_BACKEND_URL` (postgres backend) or `GOOGLE_SERVICE_ACCOUNT` (bigquery backend, the raw service-account JSON key content). The exact set is defined in `templates/secret.yaml`.

There are three ways to supply them:

### 1. Inline via values (quick/dev use)

```yaml
logflare:
  secrets:
    publicAccessToken: "..."
    privateAccessToken: "..."
    dbPassword: "..."
    dbEncryptionKey: "..."
    phxSecretKeyBase: "..."          # e.g. `openssl rand -base64 48`
    phxLiveViewSigningSalt: "..."    # e.g. `openssl rand -base64 8`
    postgresBackendUrl: "postgresql://user:pass@host:5432/db"   # postgres backend
    # googleServiceAccountJson: '{"type": "service_account", ...}'  # bigquery backend
```

Keep this in a values file that is *not* committed to source control (e.g. `secrets-values.yaml`), and pass it alongside your other values:

```sh
helm install logflare ./helm -f my-values.yaml -f secrets-values.yaml
```

This is the simplest option, but the values end up stored in the Helm release's state (in-cluster), which is not ideal for production.

### 2. `existingSecret` (recommended for production)

Create the Secret yourself, outside of Helm, then point the chart at it:

```sh
kubectl create secret generic logflare-secrets \
  --from-literal=LOGFLARE_PUBLIC_ACCESS_TOKEN=... \
  --from-literal=LOGFLARE_PRIVATE_ACCESS_TOKEN=... \
  --from-literal=DB_PASSWORD=... \
  --from-literal=LOGFLARE_DB_ENCRYPTION_KEY=... \
  --from-literal=PHX_SECRET_KEY_BASE=... \
  --from-literal=PHX_LIVE_VIEW_SIGNING_SALT=... \
  --from-literal=POSTGRES_BACKEND_URL=postgresql://user:pass@host:5432/db
  # or: --from-file=GOOGLE_SERVICE_ACCOUNT=./gcloud.json   (bigquery backend)
```

```yaml
logflare:
  existingSecret: "logflare-secrets"
```

When `existingSecret` is set, the chart does not create its own Secret (`templates/secret.yaml` renders nothing) and instead references the named Secret directly in the Deployment's `envFrom`. The Secret must contain the same keys the chart would otherwise generate — see `templates/secret.yaml` for the authoritative list per backend type.

### 3. External secret managers

Tools like Sealed Secrets or the External Secrets Operator work out of the box with this chart: point `logflare.existingSecret` at whatever Secret name your tool ultimately produces in-cluster. No chart changes are needed.

## Configurable values

| `values.yaml` key | Env var | Notes |
|---|---|---|
| `logflare.singleTenant` | `LOGFLARE_SINGLE_TENANT` | |
| `logflare.supabaseMode` | `LOGFLARE_SUPABASE_MODE` | |
| `logflare.nodeHost` | `LOGFLARE_NODE_HOST` | When empty (default), the chart injects the pod IP via the downward API instead — needed for clustering, since a fixed value would collide across pods |
| `logflare.grpcPort` | `LOGFLARE_GRPC_PORT` | |
| `logflare.httpConnectionPools` | `LOGFLARE_HTTP_CONNECTION_POOLS` | |
| `logflare.featureFlagOverride` | `LOGFLARE_FEATURE_FLAG_OVERRIDE` | |
| `logflare.phx.httpPort` | `PHX_HTTP_PORT` | |
| `logflare.phx.urlHost` | `PHX_URL_HOST` | |
| `logflare.phx.urlScheme` | `PHX_URL_SCHEME` | |
| `logflare.phx.urlPort` | `PHX_URL_PORT` | |
| `logflare.phx.checkOrigin` | `PHX_CHECK_ORIGIN` | |
| `logflare.db.hostname` | `DB_HOSTNAME` | Logflare's own metadata Postgres, distinct from the postgres event backend |
| `logflare.db.port` | `DB_PORT` | |
| `logflare.db.database` | `DB_DATABASE` | |
| `logflare.db.username` | `DB_USERNAME` | |
| `logflare.db.schema` | `DB_SCHEMA` | |
| `logflare.db.poolSize` | `DB_POOL_SIZE` | |
| `logflare.db.ssl` | `DB_SSL` | |
| `logflare.backend.type` | — | `bigquery` or `postgres`, selects which of the two blocks below is rendered |
| `logflare.backend.bigquery.projectId` | `GOOGLE_PROJECT_ID` | bigquery only |
| `logflare.backend.bigquery.projectNumber` | `GOOGLE_PROJECT_NUMBER` | bigquery only |
| `logflare.backend.bigquery.datasetIdAppend` | `GOOGLE_DATASET_ID_APPEND` | bigquery only |
| `logflare.backend.bigquery.datasetLocation` | `GOOGLE_DATASET_LOCATION` | bigquery only |
| `logflare.backend.postgres.schema` | `POSTGRES_BACKEND_SCHEMA` | postgres only |
| `logflare.secrets.*` / `logflare.existingSecret` | see [Loading secrets](#loading-secrets) | |

See `values.yaml` for the full set of generic chart values (image, service, ingress, resources, autoscaling, etc.).

## Verifying a deployment

Render the manifests locally before installing:

```sh
helm template logflare ./helm -f my-values.yaml -f secrets-values.yaml
```

Or dry-run against a real cluster:

```sh
helm install logflare ./helm -f my-values.yaml --dry-run
```

The container's liveness and readiness probes hit `/health` on the service port (default `4000`).
