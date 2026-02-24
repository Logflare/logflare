# Postgres Foreign Data Wrapper

The [Logflare Foreign Data Wrapper (FDW)](https://github.com/supabase/wrappers/tree/main/wrappers/src/fdw/logflare_fdw) allows for integration of Logflare Endpoints into PostgreSQL queries. Functionality is provided by the [Supabase Wrappers framework](https://supabase.github.io/wrappers/).

Currently, only read-only operations are supported.

Full official documentation is availabe [here](https://supabase.github.io/wrappers/logflare/)

## Example Usage

Assuming we have a Logflare Endpoint created, which accepts 3 parameters (`org_id`, `iso_timestamp_start`, and `iso_timestamp_end`), we can query the Logflare Endpoint using the following syntax.

```sql
select
  db_size,
  org_id,
  runtime_hours,
  runtime_minutes
from
  runtime_hours
where _param_org_id = 123
  and _param_iso_timestamp_start = '2023-07-01 02:03:04'
  and _param_iso_timestamp_end = '2023-07-02';
```

All parameters to be converted to query parameters in the API call must be prefixed with `_param_`.

### Sandboxed Endpoints

To query a sandboxed Logflare Endpoint, we will need to provide the `_param_sql` filter.

```sql
select
  id, event_message
from
  runtime_hours
where _param_sql = 'select id, event_message from my_cte_table where org_id = 123'
```
