with arr as (
  SELECT  (
      CASE
        WHEN @interval = '5min' THEN  `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`(5, "minute_second")
        WHEN @interval = '15min' THEN  `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`(15, "minute_minute")
        WHEN @interval = '1hr' THEN `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`(1, "hour_minute")
        WHEN @interval = '1day' THEN `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`(1, "day_hour")
        WHEN @interval = '7day' THEN `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`(7, "day_day")
        ELSE `supabase-analytics-eu.logflare.generate_timestamp_trunc_array`(1, "day_hour")
      END
  ) as d
),
dates as (
  select d from arr, unnest(`arr`.d) as d
),
agg as (
SELECT 
  dates.d as timestamp,
  count(id) as count,
  avg(m.execution_time_ms) as avg_execution_time,
  max(m.execution_time_ms) as max_execution_time,
  min(m.execution_time_ms) as min_execution_time,
  COUNTIF(r.status_code >= 400) as error_count,
  COUNTIF(r.status_code >= 100 AND r.status_code < 200 ) as one_xx_status_code_count,
  COUNTIF(r.status_code >= 200 AND r.status_code < 300 ) as two_xx_status_code_count,
  COUNTIF(r.status_code >= 300 AND r.status_code < 400 ) as three_xx_status_code_count,
  COUNTIF(r.status_code >= 400 AND r.status_code < 500 ) as four_xx_status_code_count,
  COUNTIF(r.status_code >= 500 AND r.status_code < 600 ) as five_xx_status_code_count,
  APPROX_QUANTILES(m.execution_time_ms, 100) as p95_array,
  APPROX_QUANTILES(m.execution_time_ms, 100) as p99_array,
FROM
  arr, dates left join `deno-relay-logs` on dates.d = (
        CASE
            WHEN @interval = '5min' THEN timestamp_trunc(timestamp, second)
            WHEN @interval = '15min' THEN timestamp_trunc(timestamp, minute)
            WHEN @interval = '1hr' THEN timestamp_trunc(timestamp, minute)
            WHEN @interval = '1day' THEN timestamp_trunc(timestamp, hour)
            WHEN @interval = '7day' THEN timestamp_trunc(timestamp, day)
        ELSE timestamp_trunc(timestamp, hour)
      END)
CROSS JOIN UNNEST(metadata) as m
CROSS JOIN UNNEST(m.response) as r
WHERE
  m.project_ref = @project 
  AND CASE WHEN COALESCE(@function_id, '') = '' THEN  TRUE ELSE  m.function_id = @function_id END
  and timestamp >= arr.d[offset(0)]
GROUP BY
  timestamp
ORDER BY
  timestamp ASC
)

select
  timestamp,
  min_execution_time,
  max_execution_time,
  avg_execution_time,
  p95_array[offset(95)] as p95_execution_time,
  p99_array[offset(99)] as p99_execution_time,
  count,
  error_count
from
  agg