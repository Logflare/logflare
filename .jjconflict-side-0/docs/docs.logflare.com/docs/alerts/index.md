---
title: Alerts
---

In Logflare, notifications can take one of two forms:

1. A notification of recent logs into a source (hereby referred to as **Legacy Recent Events Alerts**), or
2. a custom query triggered periodically to perform customizable checks (hereby referred to as **Query Alerts**).

When the term **Alerts** is used, it will refer to **Query Alerts**, which is the spiritual successor for the Legacy Recent Events Alerts.

Alerts is built off the querying capabilities of Logflare, and allows for teams to craft complex SQL queries for situations such as threshold warnings, near incident warnings, and periodic metrics calculations.

## Query Alerts

### Crafting the Query

Queries are executed with BigQuery SQL, and only accepts `SELECT` statements for execution, similar to Logflare Endpoints.

If there are no rows returned, the Query Alert will not continue with sending out messages to the connected integrations.

### Supported Integrations

The supported integrations are:

1. Slack (via the Logflare v2 app)
2. Webhooks

### Scheduling

Query Alerts allows for cron-based scheduling, with a minimum interval of 5 minutes. It is advised to keep the queried time range to the bare minimum required so as to reduce query execution time, as queries that cover large time ranges may scan a lot of data and result in a slow query.

### Testing

To test if your Query Alert is working, you can use the **Manual Trigger** button, which will execute the query and dispatch the results to the connected integrations.

### Example Usage

For example, if we are sending the following events to a source called `my.source`:

```json
{
  "event_message": "Hello from Logflare!",
  "stats": { "counter": 1 },
  "metadata": { "from": "docs" }
}
```

We can then create an alert with the following query with BigQuery SQL:

```sql
select sum(s.counter) as docs_total_hits
from `my.source` t
cross join unnest(t.stats) as s
cross join unnest(t.metadata) as m
where t.timestamp >= '2024-05-05'
    and m.from = 'docs'
```

Queries follow the same structure as needed when [querying a BigQuery Backend](https://docs.logflare.app/backends/bigquery/#querying-in-bigquery), thus we will need to `unnest` the repeated records.

We will also need to include a filter over the `timestamp` field, as BigQuery tables will be [partitioned by Logflare](https://docs.logflare.app/backends/bigquery/#partitioning-and-retention).

If we were to add a webhook integration to this backend, the provided url will receive a `POST` request with the following payload in the body:

```json
{
  "result": [
        { "docs_total_hits": 1 },
        ...
    ]
}
```

### Best Practices

1. Always ensure that your query's `timestamp` range is minimal. This will help to ensure that your queries run fast. For rolling metrics, use the [BigQuery timestamp functions](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions) to avoid having to hardcode dates.

2. Always add in a `LIMIT` to your query for it to be human readable. All rows returned from the query will be sent to the integrations as is, with a hard maximum limit of 1000 rows. However, to ensure readability and usability, we advise returning the minimum number of rows to achieve your goals.
