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

Query Alerts allows for cron-based scheduling, with a minimum interval of 15 minutes. It is advised to keep the queried time range to the bare minimum required so as to reduce query execution time, as queries that cover large time ranges may scan a lot of data and result in a slow query.

### Testing

To test if your Query Alert is working, you can use the **Manual Trigger** button, which will execute the query and dispatch the results to the connected integrations.
