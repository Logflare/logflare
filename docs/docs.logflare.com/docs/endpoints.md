---
sidebar_position: 4
---

# Endpoints

Logflare Endpoints creates GET HTTP API endpoints that executes a SQL query and returns the results as a JSON response.

:::note
This feature is in Alpha stage and is only available for private preview.
:::

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
) select * from errors

```

If no `sql=` query parameter is provided, the default SQL query `select * from errors` is executed.

The Endpoint consumer can pass in the following query parameter to query across the sandboxed result.

```text
?sql=select * from errors where count > 50
```

## HTTP Response

The result of the query will be returned on the `result` key of the response payload.

## Security

Endpoints are unsecure by default. However, you can generate access tokens to secure the API endpoints.
