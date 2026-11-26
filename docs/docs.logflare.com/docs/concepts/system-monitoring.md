---
sidebar_position: 6
---

# System Monitoring

System Monitoring collects metrics and logs about your sources, backends, and endpoints. Dedicated system sources are provisioned to capture such operational data:

- **`system.metrics`** - OpenTelemetry metrics for ingestion, queries, and egress
- **`system.logs`** - Application logs for your sources and backends
- **`system.traces`** - Queries executed by Logflare on your behalf

System sources behave like regular sources. Query, search, and monitor them with standard Logflare tools. They appear as favorites by default.

## Enabling System Monitoring

1. Navigate to **Account Settings** at `/account/edit`
2. Find the **"System monitoring"** section
3. Check **"Enable system monitoring"**
4. Click **"Update account"**

Logflare creates the three system sources and starts collecting data every 60 seconds. Disabling stops data collection immediately.

## System Sources

To prevent infinite looping behaviour, system sources **do not** collect metrics or logs about themselves. Ingestion metrics about system sources will not be stored within a user's `system.metrics`, and logs relating to system sources will not be stored in a user's `system.logs`. These metrics and logs will be present within the server metrics and logs when self-hosting.

### system.metrics

Contains OpenTelemetry metrics as structured events. Each metric includes:

- **`event_message`** - Metric name
- **`attributes`** - Key-value pairs with metric dimensions and values
- **`timestamp`** - When the metric was recorded

Metrics are collected every 60 seconds.

### system.logs

Contains application logs related to your sources, backends, and endpoints.

### system.traces

Contains one event per query that Logflare executes on your behalf. Tracing is best effort: under heavy load, some traces may be dropped. The following queries are traced:

- **Endpoint queries** (`query.endpoint`), including background refreshes of cached results. Requests served from the results cache are not traced.
- **Log search queries** (`query.search`): the events query, the chart query, and the query behind the event context view. A live tail re-runs its queries every second; only the first query of a tail is traced.
- **Alert queries** (`query.alert`): scheduled runs, runs started with "Trigger now", and queries run from the alert editor.
- **Single event fetches** (`query.event`): the query that loads one event, for example when opening an event from search.
- **Provisioning queries** (`query.provision`): the statements that create a ClickHouse backend's tables, a Postgres backend's table and index for each source, and BigQuery API calls that create, change, or delete a source's table or create your dataset. Create steps run each time a backend or source starts; each is traced once per Logflare server after it succeeds (for BigQuery, "already exists" counts as success), and every failed run is traced. BigQuery table changes and deletions are traced every time. Provisioning of system sources is not traced. BigQuery schema updates made while ingesting new fields are not traced.

Each event includes:

- **`event_message`** - Query kind: `query.endpoint`, `query.search`, `query.alert`, `query.event`, or `query.provision`
- **`query`** - The SQL sent to the backend adaptor. For endpoints, the adaptor may still rewrite parameter placeholders before running it. Absent for BigQuery provisioning, which uses API calls instead of SQL
- **`backend_type`** - The backend that ran the query
- **`backend_id`** - The backend that ran the query, for endpoint and provisioning queries
- **`total_bytes_processed`** - Bytes scanned, for backends that report it (BigQuery, ClickHouse)
- **`duration_ms`** - Time spent waiting for the backend adaptor to run the query
- **`status`** and **`error`** - `ok` or `error`, with the error message

Endpoint query events also include:

- **`endpoint_language`** - Language the endpoint is written in: `bq_sql`, `pg_sql`, or `ch_sql`. The query may run in another dialect, see `backend_type`
- **`params`** - Values of the parameters declared in the query, as a list of `name` and `value` records. Values are strings. Credentials such as `api_key` are never stored
- **`labels`** - Endpoint labels, as a list of `name` and `value` records
- **`endpoint_id`** and **`endpoint_uuid`** - The endpoint that ran the query

Log search events also include:

- **`search_kind`** - `events`, `aggregates` (the chart), or `context` (the event context view)
- **`lql`** - The LQL query as typed in the search bar
- **`source_id`**, **`source_token`**, and **`source_name`** - The source that was searched

Alert query events also include:

- **`alert_id`** and **`alert_name`** - The alert that ran the query. A query run from the editor before the alert is saved has no `alert_id`
- **`alert_language`** - Language the alert is written in
- **`alert_trigger`** - `scheduled` for runs on the alert schedule, `manual` for runs started by a user

Provisioning events also include:

- **`operation`** - For BigQuery, the API call: `datasets.insert`, `tables.insert`, `tables.patch`, or `tables.delete`
- **`source_id`**, **`source_token`**, and **`source_name`** - For Postgres and BigQuery table steps, the source whose table was provisioned

Single event fetch events also include:

- **`event_id`** - The ID of the fetched event
- **`lql`**, **`source_id`**, **`source_token`**, and **`source_name`** - As for log search events

To find the traces of one tenant, filter on a parameter value. For example, on BigQuery:

```sql
select timestamp, query, total_bytes_processed
from `system.traces` t
where exists (select 1 from unnest(t.params) p where p.name = 'project' and p.value = 'abc')
```

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
