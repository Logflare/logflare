---
sidebar_position: 4
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

# requires authentication
GET  https://api.logflare.app/api/endpoints/query/my.custom.endpoint
```

Querying by name requires authentication to be enabled and for a valid access token to be provided.

OpenAPI documentation for querying Logflare Endpoints can be found [here](https://logflare.app/swaggerui#/Public).

## Crafting a Query

Queries will be passed to the underlying backend to perform the querying.

### Parameters

Queries can contain parameters, which are declared with the `@` prefix. Matching query parameters are then interpolated into the query. Query parameters are decoded and interpolated as the decoded data type.

For example, given the following query:

```sql
select * from logs
where logs.name = @name and logs.age > @min_age
```

```text
?name=John+Doe&min_age=13&country=US
```

The resulting executed query for the HTTP request will be as follows:

```sql
select * from logs
where logs.name = "John Doe" and logs.age > 13
```

Parameters that do not match any that are declared in the SQL template will be ignored.

### Query Sandboxing

You can create sandboxed queries by using a CTE within the query. It allows the Endpoint consumer to provide a custom SQL query through the `sql=` query parameter.

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

## Security

Endpoints are unsecure by default. However, you can generate [access tokens](/concepts/access-tokens) to secure the API endpoints.
