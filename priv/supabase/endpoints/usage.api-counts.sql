with 
dates as (
  select (case
    when @interval = 'hourly' then timestamp_sub(current_timestamp(), interval 1 hour)
    when @interval = 'daily' then timestamp_sub(current_timestamp(), interval 7 day)
    when @interval = 'minutely' then timestamp_sub(current_timestamp(), interval 60 minute)
  end) as start
),
chart_counts as (
select
  (case
    when @interval = 'hourly' then timestamp_trunc(f0.timestamp,  hour)
    when @interval = 'daily' then timestamp_trunc(f0.timestamp,  day)
    when @interval = 'minutely' then timestamp_trunc(f0.timestamp,  minute)
  end
  ) as timestamp,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/rest')) as total_rest_requests,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/storage')) as total_storage_requests,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/auth')) as total_auth_requests,
  COUNTIF(REGEXP_CONTAINS(f2.path, '/realtime')) as total_realtime_requests,
FROM
  dates, 
  `cloudflare.logs.staging` as f0
  LEFT JOIN UNNEST(metadata) AS f1 ON TRUE
  LEFT JOIN UNNEST(f1.request) AS f2 ON TRUE
where
  REGEXP_CONTAINS(f2.url, @project) AND f0.timestamp >= dates[0]
  -- project = @project
GROUP BY
    timestamp
)
SELECT
    datetime(chart_counts.timestamp, 'UTC') as timestamp,
    COALESCE(SUM(chart_counts.total_rest_requests), 0) as total_rest_requests,
    COALESCE(SUM(chart_counts.total_storage_requests), 0) as total_storage_requests,
    COALESCE(SUM(chart_counts.total_auth_requests), 0) as total_auth_requests,
    COALESCE(SUM(chart_counts.total_realtime_requests), 0) as total_realtime_requests,
FROM  
  chart_counts
GROUP BY
    timestamp
ORDER BY
    timestamp asc;