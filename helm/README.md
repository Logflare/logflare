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

The chart needs several sensitive values, each configured under its own key in `logflare.secrets.*`:

| `logflare.secrets.*` key | Env var | Notes |
|---|---|---|
| `publicAccessToken` | `LOGFLARE_PUBLIC_ACCESS_TOKEN` | |
| `privateAccessToken` | `LOGFLARE_PRIVATE_ACCESS_TOKEN` | |
| `dbPassword` | `DB_PASSWORD` | |
| `dbEncryptionKey` | `LOGFLARE_DB_ENCRYPTION_KEY` | |
| `phxSecretKeyBase` | `PHX_SECRET_KEY_BASE` | e.g. `openssl rand -base64 48` |
| `phxLiveViewSigningSalt` | `PHX_LIVE_VIEW_SIGNING_SALT` | e.g. `openssl rand -base64 8` |
| `postgresBackendUrl` | `POSTGRES_BACKEND_URL` | postgres backend only |
| `googleServiceAccountJson` | `GOOGLE_SERVICE_ACCOUNT` | bigquery backend only, the raw service-account JSON key content |

Each field is its own object with three sub-keys:

```yaml
logflare:
  secrets:
    <field>:
      value: ""               # inline value, rendered into the chart's own Secret
      existingSecret: ""      # OR: name of a Secret that already exists in the namespace
      existingSecretKey: ""   # key within that Secret (defaults to the chart's own key name above)
```

For each field independently: if `existingSecret` is empty, the chart renders that key into the Secret it creates (named after the release) using `value`. If `existingSecret` is set, the chart instead wires that env var via `secretKeyRef` straight to `existingSecret`/`existingSecretKey`, and **does not** render that key into its own Secret. You can mix both approaches — e.g. keep `dbPassword` inline while sourcing `googleServiceAccountJson` from an externally managed Secret.

### 1. Inline via values (quick/dev use)

```yaml
logflare:
  secrets:
    publicAccessToken:
      value: "..."
    privateAccessToken:
      value: "..."
    dbPassword:
      value: "..."
    dbEncryptionKey:
      value: "..."
    phxSecretKeyBase:
      value: "..."          # e.g. `openssl rand -base64 48`
    phxLiveViewSigningSalt:
      value: "..."          # e.g. `openssl rand -base64 8`
    postgresBackendUrl:
      value: "postgresql://user:pass@host:5432/db"   # postgres backend
    # googleServiceAccountJson:
    #   value: '{"type": "service_account", ...}'    # bigquery backend
```

Keep this in a values file that is *not* committed to source control (e.g. `secrets-values.yaml`), and pass it alongside your other values:

```sh
helm install logflare ./helm -f my-values.yaml -f secrets-values.yaml
```

This is the simplest option, but the values end up stored in the Helm release's state (in-cluster), which is not ideal for production.

### 2. Referencing an existing Secret (recommended for production)

Create the Secret yourself, outside of Helm, then point the relevant field(s) at it via `existingSecret`/`existingSecretKey`. Unlike the previous whole-chart `existingSecret` toggle, this is per field — the Secret's key names don't need to match the chart's own, and different fields can point at entirely different Secrets:

```sh
kubectl create secret generic logflare-secrets \
  --from-literal=public-access-token=... \
  --from-literal=private-access-token=... \
  --from-literal=db-password=...
```

```yaml
logflare:
  secrets:
    publicAccessToken:
      existingSecret: "logflare-secrets"
      existingSecretKey: "public-access-token"
    privateAccessToken:
      existingSecret: "logflare-secrets"
      existingSecretKey: "private-access-token"
    dbPassword:
      existingSecret: "logflare-secrets"
      existingSecretKey: "db-password"
    # remaining fields fall back to `value` / the chart's own Secret
```

### 3. External Secrets Operator (ESO)

[External Secrets Operator](https://external-secrets.io/) is the recommended way to run this in production: it syncs values from a real secret manager (AWS Secrets Manager, GCP Secret Manager, Vault, etc.) into a plain Kubernetes `Secret` that this chart then references via `existingSecret`.

1. **Provision an `ExternalSecret`** (outside this chart, e.g. in your cluster-config repo) that materializes the Secret this chart will consume:

    ```yaml
    apiVersion: external-secrets.io/v1
    kind: ExternalSecret
    metadata:
      name: logflare-secrets
      namespace: my-namespace
    spec:
      secretStoreRef:
        kind: ClusterSecretStore
        name: aws-secrets-store
      refreshPolicy: Periodic
      refreshInterval: 1h
      target:
        name: logflare-secrets
        creationPolicy: Owner
        deletionPolicy: Delete
      dataFrom:
        - extract:
            key: prod/logflare/secrets
    ```

    This produces a Secret named `logflare-secrets` in the namespace, with whatever keys exist at `prod/logflare/secrets` in your secret store (e.g. `public_access_token`, `private_access_token`, `db_password`, ...). ESO owns creation/rotation of this Secret entirely; the chart never sees the underlying values.

2. **Point the chart's fields at it**, matching each field to the key your secret store uses:

    ```yaml
    logflare:
      secrets:
        publicAccessToken:
          existingSecret: "logflare-secrets"
          existingSecretKey: "public_access_token"
        privateAccessToken:
          existingSecret: "logflare-secrets"
          existingSecretKey: "private_access_token"
        dbPassword:
          existingSecret: "logflare-secrets"
          existingSecretKey: "db_password"
        dbEncryptionKey:
          existingSecret: "logflare-secrets"
          existingSecretKey: "db_encryption_key"
        phxSecretKeyBase:
          existingSecret: "logflare-secrets"
          existingSecretKey: "phx_secret_key_base"
        phxLiveViewSigningSalt:
          existingSecret: "logflare-secrets"
          existingSecretKey: "phx_live_view_signing_salt"
        # bigquery backend:
        googleServiceAccountJson:
          existingSecret: "logflare-secrets"
          existingSecretKey: "google_service_account_json"
    ```

3. **Install/upgrade as usual** — no chart changes are needed, since the Deployment always wires each field via `secretKeyRef`:

    ```sh
    helm upgrade --install logflare ./helm -f my-values.yaml
    ```

Because each field is wired independently, you can also split fields across multiple `ExternalSecret`/Secret objects (e.g. one per upstream secret-store path) — just set a different `existingSecret` name per field.

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
| `logflare.secrets.*` | see [Loading secrets](#loading-secrets) | |

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
