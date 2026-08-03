---
sidebar_position: 4
---

# Querying

To run adhoc queries for exploratory analysis, use the Querying or Search functionality. The use case for this differs from other features that Logflare offers:

- For periodic query-based checks and data push integrations, use the Alerting functionality.
- For building GET APIs for downstream programmatic consumption and data pull integrations, use the Endpoints functionality

:::info

You will need to use an access token with the `management` scope to query the management API. 
An `ingest` or `query` scoped token **cannot** be used to for this querying API.   
:::


## Via Management API

Sources can be queried through SQL using our management API.

The following query parameters are available:

- `?sql=` (string): the SQL query string. 
- `?backend_id=` (integer): optional backend to execute against. When provided, the backend type determines the SQL language.
- `?bq_sql=`, `?ch_sql=`, `?pg_sql=` (string): deprecated language-specific SQL parameters. They remain supported for backwards compatibility, but new integrations should use `?sql=`. Takes precedence over SQL language determined from `backend_id`.

If `backend_id` is omitted, the query runs against the default BigQuery backend using BigQuery SQL. Deprecated `ch_sql` and `pg_sql` requests retain their legacy backend selection behavior for backwards compatibility. For new PostgreSQL or ClickHouse queries, pass the SQL string using `sql` and include the `backend_id`.

```
# Endpoint
GET https://api.logflare.app/api/query?sql=...

# With a query
GET https://api.logflare.app/api/query?sql=select id, event_message, datetime(timestamp) as timestamp from `my_source` where timestamp > '2024-01-01'
```

## Caveats and Limitations

The following caveats apply when querying this management API route:

- Due to the partitioning that Logflare performs, queries must have a `WHERE` filter over the `timestamp` field at all times.
- A hard maximum of 1000 rows will be returned for the BigQuery backend.
