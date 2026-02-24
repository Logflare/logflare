---
sidebar_position: 5
---

# Endpoints

Logflare Endpoints creates GET HTTP API endpoints that executes a SQL query and returns the results as a JSON response.

:::note
This feature is in Alpha stage and is only available for private preview.
:::

## API Endpoints

There are two ways to query a Logflare Endpoint, via the Endpoint UUID or via the endpoint's name:

```
GET  https://api.logflare.app/api/endpoints/query/9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3
POST  https://api.logflare.app/api/endpoints/query/9dd9a6f6-8e9b-4fa4-b682-4f2f5cd99da3

# requires authentication
GET  https://api.logflare.app/api/endpoints/query/my.custom.endpoint
POST  https://api.logflare.app/api/endpoints/query/my.custom.endpoint
```

Querying by name requires authentication to be enabled and for a valid access token to be provided.

OpenAPI documentation for querying Logflare Endpoints can be found [here](https://logflare.app/swaggerui#/Public).

## Crafting a Query

Queries will be passed to the underlying backend to perform the querying.

### Parameters

Queries can contain parameters, which are declared with the `@` prefix. Matching query parameters are then interpolated into the query. Query parameters are decoded and interpolated as the decoded data type.

For example, given the following query:

```sql
select id, event_message, timestamp, metadata from logs
where logs.name = @name and logs.age > @min_age
```

```text
?name=John+Doe&min_age=13&country=US
```

The resulting executed query for the HTTP request will be as follows:

```sql
select id, event_message, timestamp, metadata from logs
where logs.name = "John Doe" and logs.age > 13
```

Parameters that do not match any that are declared in the SQL template will be ignored.

### Query Sandboxing

You can create sandboxed queries by using a CTE within the query. It allows the Endpoint consumer to provide a custom SQL query through the `sql=` query parameter.

:::note
Sandboxed queries are supported for BigQuery and ClickHouse backends. PostgreSQL backends do not currently support this feature.
:::

For example, this sandboxed query creates a temporary result called `errors`, which limits the results to containing the `"ERROR"` string as well as being before the year `2020` .

```sql
with errors (
    select event_message as err, count(id) as count as msg
    from my_source_name
    where regexp_contains(event_message, "ERROR") and timestamp >= "2020-01-01"
    group by event_message
    order by count desc
) select err from errors

```

If no `sql=` query parameter is provided, the default SQL query `select err from errors` is executed.

The Endpoint consumer can pass in the following query parameter to query across the sandboxed result.

```text
?sql=select err from errors where regexp_contains(err, "my_error")
```

Should large SQL queries need to be executed, the SQL query string can be placed in the GET JSON body. Logflare will read the body and use the `sql` field in the body payload. This will only occur if **no `?sql=` query parameter is present in the request's query parameters**. This behaviour does not extend to other declared parameters (such as `@my_param`), and only applies to the special `sql` query parameter for sandboxed endpoints.

## HTTP Response

The result of the query will be returned on the `result` key of the response payload.

```
{
    "result": [{err: "my erorr message"}]
}
```

## Cache

All endpoint queries by default are set to cache results for 3,600 seconds. The first API request that hits Logflare will create and set the results of the cache for the configured cache duration. Caching is recommended to keep querying costs down while ensuring the fastest possible return of results to the end user.

To disable the cache, set the cached duration to `0`. However, it is not recommended to do so unless you absolutely need up-to-date results. To prevent stale data while keeping the cache warm, use the cache proactive requerying feature.

Caching is performed on a query parameter basis. As such, if there are three API requests sent to an endpoint, `?path=123`, `?path=223`, and `?other=value`, this will result in 3 difference caches being created.

### Proactive Requerying

Logflare endpoints can be proactively requeried to ensure that the cache does not become stale throughout the cache lifetime.

When configured, the cache will be automatically updated at the set interval, performing only one query to update the cached data.

## Query Tagging with Labels

Endpoints support query labeling for tracking and monitoring in the backend. Labels are configured as a comma-separated allowlist and can reference parameters (`@my_param`), values provided in the `LF-ENDPOINT-LABELS` request header, or static values.

### Configuration Format

```text
static_key=static_value,param_key=@param_name,header_only_key
```

### Label Sources (by precedence)

1. **Query parameters** - `@param_name` references take values from URL parameters
2. **Request headers** - `LF-ENDPOINT-LABELS: key=value,key2=value2`
3. **Static values** - Fixed values in the configuration

### Example

**Configuration:** `user_id=@user_id,environment=production,session_id`

**Request:**

```bash
GET /api/endpoints/query/my-endpoint?user_id=123
LF-ENDPOINT-LABELS: session_id=abc123,ignored=xyz
```

**Resulting labels:**

```json
{
  "user_id": "123",
  "environment": "production",
  "session_id": "abc123"
}
```

Only allowlisted labels are processed. Query parameters override header values for the same key.

## Subquery Expansion with Other Endpoints

Logflare endpoints support subquery expansion, allowing you to reference and query data from other endpoints within your SQL queries. This enables powerful data composition and cross-endpoint analysis.

To reference another endpoint in your query, use the endpoint or alert's name as the table reference:

```sql
-- my-base-endpoint
select my_field, count(id) as counts from `my-source`
where my_data > @value
group by my_field

```

```sql
-- final endpoint
select my_field, counts from `my-endpoint`
```

The underlying base endpoint reference will get expanded at runtime to a subquery. Any endpoint parameters referenced using `@` will be extended to the parent endpoint as well.

In this case, the `@value` parameter will be required by the final endpoint as well.

## Security

Endpoints are unsecure by default. However, you can generate [access tokens](/concepts/access-tokens) to secure the API endpoints.

## PII Redaction

Logflare endpoints support automatic redaction of personally identifiable information (PII) from query results to help protect sensitive data. When enabled, the PII redaction feature will automatically replace IP addresses in query result values with "REDACTED".

PII redaction can be enabled when checking the "Redact PII from query results" checkbox when configuring the endpoint. Override per request with `LF-ENDPOINT-REDACT-PII: true|false`; if omitted, the endpoint setting is used.

Currently, PII redaction targets:

- IPv4 addresses
- IPv6 addresses

Redaction occurs post-query without affecting performance, only targets string values, and recursively processes nested structures for comprehensive PII protection.

### Example

**With PII redaction enabled:**

```json
{
  "result": [
    {
      "user_id": 123,
      "client_ip": "REDACTED",
      "message": "User connected from REDACTED",
      "metadata": {
        "session_data": {
          "origin_ip": "REDACTED"
        }
      }
    }
  ]
}
```
