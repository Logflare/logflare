with arr as (
  SELECT `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`((
      CASE
        WHEN @interval = 'hourly' THEN 1
        WHEN @interval = 'daily' THEN 7
        WHEN @interval = 'minutely' THEN 1
        ELSE 1
      END
  ), (
      CASE 
        WHEN @interval = 'hourly' THEN "day_hour"
        WHEN @interval = 'daily' THEN "day_day"
        WHEN @interval = 'minutely' THEN "hour_minute"
        ELSE "day_hour"
      END 
  )) as d
),
dates as (
  select 
    d,
    arr.d as arr_d
  from arr, unnest(`arr`.d) as d
  where d != arr.d[offset(0)]
),
logs as (
select
  (
        CASE
            WHEN @interval = 'hourly' THEN timestamp_trunc(f0.timestamp, hour)
            WHEN @interval = 'daily' THEN timestamp_trunc(f0.timestamp, day)
            WHEN @interval = 'minutely' THEN timestamp_trunc(f0.timestamp, minute)
        ELSE timestamp_trunc(f0.timestamp, hour)
      END
) as timestamp,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/rest')) as total_rest_requests,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/storage')) as total_storage_requests,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/auth')) as total_auth_requests,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/realtime')) as total_realtime_requests,
FROM
  `cloudflare.logs.prod` as f0
  LEFT JOIN UNNEST(metadata) AS f1 ON TRUE
  LEFT JOIN UNNEST(f1.request) AS f2 ON TRUE
WHERE
    project = @project
GROUP BY timestamp
)
SELECT
    dates.d as timestamp,
    COALESCE(SUM(logs.total_rest_requests), 0) as total_rest_requests,
    COALESCE(SUM(logs.total_storage_requests), 0) as total_storage_requests,
    COALESCE(SUM(logs.total_auth_requests), 0) as total_auth_requests,
    COALESCE(SUM(logs.total_realtime_requests), 0) as total_realtime_requests
FROM
    dates
    LEFT JOIN logs on dates.d = logs.timestamp
    and timestamp >= dates.arr_d[offset(0)]
GROUP BY
    timestamp
ORDER BY
    timestamp asc;