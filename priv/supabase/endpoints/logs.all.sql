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

edge_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `cloudflare.logs.prod` as t
  cross join unnest(metadata) as m
  cross join unnest(m.request) as r
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  t.project = @project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

postgres_logs as (
  select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata
from retention, `postgres.logs` as t
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  t.project = @project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.timestamp > retention.date
  order by
  timestamp desc
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
  CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  and m.project_ref = @project
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

function_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `deno-subhosting-events` as t
  cross join unnest(t.metadata) as m
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  m.project_ref = @project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

auth_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `gotrue.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  -- order of the where clauses matters
  -- project then timestamp then everything else
  -- m.project = @project
  t.project = @project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

realtime_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `realtime.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  m.project = @project 
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

storage_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `storage.logs.prod.2` as t
  cross join unnest(t.metadata) as m
where
  m.project = @project
  AND CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

postgrest_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `postgREST.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.project = @project
  AND t.timestamp > retention.date
order by
  t.timestamp desc
),

pgbouncer_logs as (
select 
  t.timestamp,
  t.id, 
  t.event_message, 
  t.metadata 
from retention, `pgbouncer.logs.prod` as t
  cross join unnest(t.metadata) as m
where
  CASE WHEN COALESCE(@iso_timestamp_start, '') = '' THEN  TRUE ELSE  t.timestamp > @iso_timestamp_start END
  AND CASE WHEN COALESCE(@iso_timestamp_end, '') = '' THEN TRUE ELSE t.timestamp <= @iso_timestamp_end END
  AND t.project = @project
  AND t.timestamp > retention.date
order by
  t.timestamp desc
)

SELECT id, timestamp, event_message, metadata
FROM edge_logs
LIMIT 100