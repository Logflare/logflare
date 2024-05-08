---
toc_max_heading_level: 3
---

# PostgreSQL

Logflare has experimental support for storing and querying log events to a PostgreSQL server. Ingested logs are directly inserted into tables, and each source maps to a Postgres table within a given schema.

:::warning
PostgreSQL as a backend is only available for the **V2 ingestion** and currently has limited functionality and support. Ingestion architecture and underlying storage mechanisms are subject to **breaking changes**.
:::

## Behavior and Configuration

On source creation, relevant migrations to create the source's table will be performed on the database.

The table schema is as follows:

- `id`: The log event UUID.
- `timestamp`: Unix microsecond, stored as `bigint`
- `event_message`: The provided or generated event message of the log event, stored as `text`
- `body`: A the processed log event, stored as `jsonb`

:::note
Where possible, storage and querying behavior will follow the [BigQuery](../bigquery) behavior
:::

### User-Provided Configuration

The following values can be provided to the backend:

- `url` (`string`, optional): A PostgreSQL connection string, for connecting to the server.
- `schema` (`string`, optional): The schema to scope all Logflare-related operations to.
- `username` (`string`), optional: Username to connect as. Cannot be used with `url`.
- `password` (`string`), optional: Password for user authentication. Cannot be used with `url`.
- `port` (`string`), optional: Port of the database server. Cannot be used with `url`.
- `hostname` (`string`), optional: Hostname of the database server. Cannot be used with `url`.
- `hostname` (`string`), optional: Hostname of the database server. Cannot be used with `url`.
- `pool_size` (`integer`), optional: Sets the number of connections in the connection pool to the database server. Defaults to 1.

If a `url` is provided, it cannot be used in conjunction with username/password credentials.

Either `url` or username/password credentials must be provided.

Configure `pool_size` if you wish to increase the throughput of ingestion and reduce chance of the connection pool being empty during ingest.

### API Querying
It is possible to query the PostgreSQL backend using PostreSQL dialect, using the management API querying endpoint:

```
GET https://api.logflare.app/api/
```

Valid private access tokens must be used.