---
toc_max_heading_level: 3
---

# PostgreSQL

Logflare has experimental support for storing and querying log events to a PostgreSQL server. Ingested logs are directly inserted into tables, and each source maps to a Postgres table within a given schema.

:::warning
PostgreSQL as a backend is only available for the **V2 ingestion** and currently has limited functionality.
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

- `url` (`string`, required): A PostgreSQL connection string, for connecting to the server.
- `schema` (`string`, optional): The schema to scope all Logflare-related operations to.
