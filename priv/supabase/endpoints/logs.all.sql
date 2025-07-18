with retention as (
  select (
    CASE
      WHEN @project_tier = 'FREE' THEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day)
      WHEN @project_tier = 'PRO' THEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 day)
      WHEN (@project_tier = 'PAYG' OR @project_tier = 'ENTERPRISE') THEN TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 day) 
      ELSE  TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 day) 
    END
  ) as date
),

project_param AS (
  SELECT COALESCE(@project, '"default"') AS project  -- JSON string
),

edge_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `cloudflare.logs.prod` as t
  cross join unnest(metadata) as m
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  t.project = project_param.project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND cast(t.timestamp as timestamp) > retention.date
order by
  cast(t.timestamp as timestamp) desc
),

postgres_logs as (
  select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata
from retention, project_param, `postgres.logs` as t
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  t.project = project_param.project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND cast(t.timestamp as timestamp) > retention.date
  order by cast(t.timestamp as timestamp) desc
),

function_edge_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `deno-relay-logs` as t
  cross join unnest(t.metadata) as m
where
  CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  and m.project_ref = @project
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
),

function_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `deno-subhosting-events` as t
  cross join unnest(t.metadata) as m
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  m.project_ref = project_param.project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
),

auth_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `gotrue.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  -- m.project = @project
  t.project = project_param.project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
),

realtime_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `realtime.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  m.project = project_param.project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
),

storage_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `storage.logs.prod.2` as t
  cross join unnest(t.metadata) as m
where
  m.project = project_param.project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
),

postgrest_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `postgREST.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND t.project = project_param.project
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
),

pgbouncer_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, project_param, `pgbouncer.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  cast(t.timestamp as timestamp) > cast(@iso_timestamp_start as timestamp) END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE cast(t.timestamp as timestamp) <= cast(@iso_timestamp_end as timestamp) END
  AND t.project = project_param.project
  AND cast(t.timestamp as timestamp) > retention.date
order by cast(t.timestamp as timestamp) desc
)

SELECT id, timestamp, event_message, metadata
FROM edge_logs
LIMIT 100