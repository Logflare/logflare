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

**This chart never creates or owns a `Secret`.** You provision the Secret(s) yourself (e.g. via `kubectl create secret`, or via a tool like External Secrets Operator) with keys already named the way Logflare expects, and list their names in `logflare.secretRefs`. Every key in each listed Secret is injected in bulk via `envFrom`, the same way the chart's own ConfigMap is:

```yaml
logflare:
  secretRefs:
    - logflare-secrets
```

Unlike a per-variable mapping, this means **the Secret's key names must exactly match the env vars Logflare reads** (see the table below) — there's no renaming step. If you need to source individual vars from differently-named keys, or split them across Secrets with arbitrary key names, create multiple Secrets with the right key names upstream (e.g. via multiple `ExternalSecret` objects) and list all of them in `secretRefs`.

Most deployments need at least these env vars, so your Secret(s) should contain these keys:

| Env var | Notes |
|---|---|
| `LOGFLARE_PUBLIC_ACCESS_TOKEN` | |
| `LOGFLARE_PRIVATE_ACCESS_TOKEN` | |
| `DB_PASSWORD` | |
| `LOGFLARE_DB_ENCRYPTION_KEY` | |
| `PHX_SECRET_KEY_BASE` | e.g. `openssl rand -base64 48` |
| `PHX_LIVE_VIEW_SIGNING_SALT` | e.g. `openssl rand -base64 8` |
| `POSTGRES_BACKEND_URL` | postgres backend only |
| `GOOGLE_SERVICE_ACCOUNT` | bigquery backend only, the raw service-account JSON key content |

### Creating the Secret yourself

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
  secretRefs:
    - logflare-secrets
```

### External Secrets Operator (ESO)

[External Secrets Operator](https://external-secrets.io/) is the recommended way to run this in production: it syncs values from a real secret manager (AWS Secrets Manager, GCP Secret Manager, Vault, etc.) into a plain Kubernetes `Secret`, which this chart then references via `logflare.secretRefs`.

1. **Provision an `ExternalSecret`** (outside this chart, e.g. in your cluster-config repo) that materializes the Secret this chart will consume, with keys already named to match what Logflare expects:

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

    If the secrets in your secret store at `prod/logflare/secrets` don't already use Logflare's expected key names (e.g. `public_access_token` instead of `LOGFLARE_PUBLIC_ACCESS_TOKEN`), use `data:` entries with explicit `secretKey`/`remoteRef` pairs (or a templated `target.template`) instead of `dataFrom.extract`, so the resulting Secret's keys match exactly — ESO owns creation/rotation of this Secret entirely, and the chart injects whatever keys land in it verbatim.

2. **List the Secret's name in `logflare.secretRefs`:**

    ```yaml
    logflare:
      secretRefs:
        - logflare-secrets
    ```

3. **Install/upgrade as usual** — no chart changes are needed, since the Deployment always wires every listed Secret in via `envFrom.secretRef`:

    ```sh
    helm upgrade --install logflare ./helm -f my-values.yaml
    ```

You can list multiple Secret names in `secretRefs` (e.g. one per `ExternalSecret`/upstream secret-store path) — all of their keys are injected together.

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
| `logflare.secretRefs` | see [Loading secrets](#loading-secrets) | |
| `logflare.certFilesSecret` | — | Name of a Secret whose keys are cert filenames; mounted as files. See [Certificate files](#certificate-files) |
| `logflare.certFilesMountPath` | `DB_SSL_*_PATH`, `LOGFLARE_TLS_*_PATH` | Mount path for `certFilesSecret`; the chart points the cert path env vars here |
| `logflare.reloader` | — | When `true`, adds the Stakater Reloader annotation so the Deployment rolls on ConfigMap/Secret changes |
| `logflare.extraConfig` | (any) | Map of non-secret env vars rendered verbatim into the ConfigMap |

See `values.yaml` for the full set of generic chart values (image, service, ingress, resources, autoscaling, etc.).

### Certificate files

Unlike the env-var secrets loaded via `envFrom`, Logflare reads its TLS/mTLS
material from files on disk: the internal database SSL certs (when `DB_SSL` is
enabled) and the gRPC TLS cert/key (when `LOGFLARE_ENABLE_GRPC_SSL` is enabled).

Provide these via a separate Secret whose keys are the exact filenames —
`db-server-ca.pem`, `db-client-cert.pem`, `db-client-key.pem`, `cert.pem`,
`cert.key` — and set `logflare.certFilesSecret` to its name. The chart mounts it
at `logflare.certFilesMountPath` and sets `DB_SSL_CA_CERT_PATH`,
`DB_SSL_CLIENT_CERT_PATH`, `DB_SSL_CLIENT_KEY_PATH`, `LOGFLARE_TLS_CERT_PATH`,
and `LOGFLARE_TLS_KEY_PATH` to point at the mounted files. (The BigQuery service
account key is handled as an env-var secret via `GOOGLE_APPLICATION_CREDENTIALS_JSON`,
not a mounted file.)

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
