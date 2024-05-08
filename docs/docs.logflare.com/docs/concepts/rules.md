# Rules

Rules are logical rulesets that direct ingestion data to one or more sources or [backends](../backends). This allows us to copy subsets of ingested data to multiple sources, or to forward data to external backends.

Filtering syntax used is the [Logflare Query Language](../concepts/lql).

## Source Rules

Source rules allow for source-to-source filtering, where a source will send data to a given destination source.

## Drain Rules

Drain rules allow for source-to-backend filtering, where a source will send data to a given destination [backend](../backend). The backend could be ingest-only, such as 3rd party services.

:::info Private Alpha Only
Drain rules and multi-backends are a private alpha only feature for the Logflare service. Please contact us if this interests you.
:::

On drain creation, data ingested into the source will automatically be routed to the selected backend.

If the backend is a supported fully-featured managed backend (such as BigQuery or Postgres), tables will automatically be created and managed as if the source had been attached to the backend. If an associated source table already exists, it would insert into the existing table.
