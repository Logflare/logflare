---
sidebar_position: 6
---

# System Monitoring

System Monitoring collects metrics and logs about your sources, backends, and endpoints. Dedicated system sources are provisioned to capture such operational data:

- **`system.metrics`** - OpenTelemetry metrics for ingestion, queries, and egress
- **`system.logs`** - Application logs for your sources and backends

System sources behave like regular sources. Query, search, and monitor them with standard Logflare tools. They appear as favorites by default.

## Enabling System Monitoring

1. Navigate to **Account Settings** at `/account/edit`
2. Find the **"System monitoring"** section
3. Check **"Enable system monitoring"**
4. Click **"Update account"**

Logflare creates the three system sources and starts collecting data every 60 seconds. Disabling stops data collection immediately.

## System Sources

To prevent infinite looping behaviour, system sources **do not** collect metrics or logs about themselves. Ingestion metrics about system sources will not be stored within a user's `system.metrics`, and logs relating to system sources will not be stored in a user's `system.logs`. These metrics and logs will be present within the server metrics and logs.

### system.metrics

Contains OpenTelemetry metrics as structured events. Each metric includes:

- **`event_message`** - Metric name
- **`attributes`** - Key-value pairs with metric dimensions and values
- **`timestamp`** - When the metric was recorded

Metrics are collected every 60 seconds.

### system.logs

Contains application logs related to your sources, backends, and endpoints.

## Metrics Collected

| Metric                                           | Description                                                                  | Metadata                                 |
| ------------------------------------------------ | ---------------------------------------------------------------------------- | ---------------------------------------- |
| `logflare.backends.ingest.ingested_bytes`        | Total bytes ingested per source. Tracks storage consumption.                 | `source_id`, `backend_id`, custom labels |
| `logflare.backends.ingest.ingested_count`        | Count of events ingested per source. Tracks ingestion volume.                | `source_id`, `backend_id`, custom labels |
| `logflare.endpoints.query.total_bytes_processed` | Bytes processed when executing endpoint queries. Tracks query costs.         | `endpoint_id`, custom labels             |
| `logflare.backends.ingest.egress.request_bytes`  | Bytes sent to external HTTP endpoints and webhooks. Tracks egress bandwidth. | Backend-specific metadata                |

## Custom Labels

Add dimensions to metrics through labels on sources and endpoints. Labels appear in the `attributes` field of metric events.

For endpoint labelling behavior, see [Query Tagging with Labels](/concepts/endpoints#query-tagging-with-labels).

### Format

Use comma-separated key-value pairs:

```
environment=production,region=us-east,team=backend
```

### Ingest-time Field Extraction

Extract values from event metadata using field paths:

```
label_name=field.path
label_name=m.field.path
```

Examples:

- `environment=m.env` extracts `metadata.env`
- `user_type=m.user.type` extracts `metadata.user.type`
- `region=region` extracts top-level `region`

Only string values are extracted. Nested maps and lists are excluded.
