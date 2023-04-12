# BigQuery

Logflare natively supports the storage of log events to BigQuery. Ingested logs are **streamed** into BigQuery, and each source is mapped to a BigQuery table.

## Behavior and Configuration

On table initialization, Logflare sets optimal configuration automatically to ensure that tables are optimized.

### Ingestion

Ingested events are [streamed](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery) into BigQuery. This allows us to maximize the throughput into BigQuery, allowing Logflare to handle large volumes of events.

:::note
Due to this streaming requirement, BYOB GCP projects **must have billing enabled**.
:::

### Partitioning and Retention

All tables are partitioned by the **timestamp** field, and are partitioned by **day**. This means that all queries across the BigQuery table must have a filter over the timestamp field.

Backend Time-To-Live (TTL) is the source setting which configures a BigQuery table's partition expiry. This setting can be configured through Source > Edit > Backend TTL.

For metered plan users, if the TTL is not set, the BigQuery table will default to **7 days**.

For users on the Free plan, the maximum retention is **3 days**.

## Logflare-Managed BigQuery

Logflare free and metered users will not need to manage BigQuery settings and permissions, and will have access to their data via their registered e-mail address.

The differences in BigQuery behavior for the two plans are as follows:

| Aspect                  | Free             | Metered       | BYOB      |
| ----------------------- | ---------------- | ------------- | --------- |
| Schema Fields           | Up to 50         | Up to 500     | Up to 500 |
| Retention (Backend TTL) | Up to 3 Days     | Up to 90 days | Unlimited |
| Events                  | 12,960,000/month | Unlimited     | Unlimited |

## Bring Your Own Backend (BYOB)

You can also Bring Your Own Backend by allowing Logflare to manage a GCP project's BigQuery.

This allows you to retain data beyond the metered plan's 90 days, as well as integrating the BigQuery tables managed by Logflare into your BigQuery-backend data warehouse.

### Setting Up Your Own BigQuery Backend

:::warn Enable Billing for Project
Enable a Google Cloud Platform billing account with payment information or [we won't be able to insert into your BigQuery table!](#ingestion-bigquery-streaming-insert-error)
:::

#### Step 1: Navigate to IAM and Add a Member

Navigate to `Google Cloud Platform > IAM & admin > IAM`

![Navigate to IAM](./navigate-to-iam.png)

Then, click on the **Add** button.

![Add a Member](./add-a-member.png)

#### Step 2: Add the Logflare Service Account

The Logflare service account is:

```
logflare@logflare-232118.iam.gserviceaccount.com
```

Assign `BigQuery Data Owner` and `BigQuery Job User` permissions to the Logflare service account.

![BigQuery Data Owner Permissions](./add-service-account-with-permissions.png)

![BigQuery Job User Permissions](./bq-job-user-permissions.png)

#### Step 3: Update Account Settings in Logflare

Find the GCP project ID in the [dashboard](https://console.cloud.google.com/home/dashboard)

![Get BigQuery Project ID](./get-project-id.png)

Navigate to the [account preferences](https://logflare.app/account/edit) and add in the GCP project ID.

![Set BigQuery Project ID](./set-project-id.png)

#### Step 4 (Optional): Update Source Settings in Logflare

You can also optionally update your sources' TTL to tweak how long you want to retain log events in BigQuery.

![Set Source TTL](./add-a-member.png)

:::note
The steps for setting up self-hosted Logflare requires different BigQuery configurations, please refer to the [self-hosted](/docs/self-hosted) documentation for more details.
:::

## Querying in BigQuery

You can directly execute SQL queries in BigQuery instead of through the Logflare UI. This would be helpful for generating reports that require aggregations, or to perform queries across multiple BigQuery tables.

When referencing Logflare-managed BigQuery tables, you will need to reference the table by the source's UUID. If you are crafting the query within [Logflare Endpoints](/docs/endpoints), the table name resolution is handled automatically for you.

### Unnesting Repeated Records

Nested columns are represeted as repeated `RECORD`s in BigQuery. To query inside a nested record you must UNNEST it like so:

```sql
SELECT timestamp, req.url, h.cf_cache_status
FROM `your_project_id.your_dataset_name.your_table_name` t
CROSS JOIN UNNEST(t.metadata) m
CROSS JOIN UNNEST(m.request) req
CROSS JOIN UNNEST(m.response) resp
CROSS JOIN UNNEST(resp.headers) h
WHERE DATE(timestamp) = "2019-05-09"
ORDER BY timestamp DESC
LIMIT 10
```

## Troubleshooting

### Ingestion: BigQuery Streaming Insert Error

Logflare uses BigQuery's [streaming insert API](https://cloud.google.com/bigquery/docs/streaming-data-into-bigquery) to provide ingestion functionality.

If you are on the BYOB plan and have configured Logflare to ingest into BigQuery using the above steps, you may receive this error:

```
Access Denied: BigQuery BigQuery: Streaming insert is not allowed in the free tier
```

To resolve this error, you will need enable billing for your project through the GCP console.
