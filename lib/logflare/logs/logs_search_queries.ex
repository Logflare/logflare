defmodule Logflare.Logs.SearchQueries do
  @moduledoc false
  import Ecto.Query
  import Logflare.EctoBigQueryFunctions
  @chart_periods ~w(day hour minute second)a

  def select_aggregates(q) do
    q
    |> select([t, ts], %{
      timestamp: fragment("? as timestamp", coalesce(t.timestamp, ts.timestamp)),
      datetime: fragment("DATETIME(?) AS datetime", coalesce(t.timestamp, ts.timestamp)),
      value: fragment("? as value", coalesce(t.value, ts.value))
    })
  end

  def limit_aggregate_chart_period(query, period) when period in @chart_periods do
    number =
      case period do
        :day -> 30
        :hour -> 168
        :minute -> 120
        :second -> 180
      end

    limit(query, ^number)
  end

  def where_timestamp_tailing(query, min, max) do
    where(query, [t, ...], t.timestamp >= ^min and t.timestamp <= ^max)
  end

  def where_streaming_buffer(query) do
    where(query, [l], in_streaming_buffer())
  end

  def where_default_tailing_events_partition(query) do
    where(
      query,
      [log],
      partition_date() >= bq_date_sub(^Date.utc_today(), 1, "day") or in_streaming_buffer()
    )
  end

  def where_default_tailing_charts_partition(query, chart_period) do
    utc_today = Date.utc_today()

    days =
      case chart_period do
        :day -> 31
        :hour -> 7
        :minute -> 1
        :second -> 1
      end

    query
    |> where(
      [t, ...],
      partition_date() >= bq_date_sub(^utc_today, ^days, "day") or in_streaming_buffer()
    )
  end

  def where_partitiondate_between(query, min, max) do
    where(
      query,
      [t, ...],
      fragment(
        "_PARTITIONDATE BETWEEN DATE_TRUNC(?, DAY) AND DATE_TRUNC(?, DAY)",
        ^Timex.to_date(min),
        ^Timex.to_date(max)
      ) or in_streaming_buffer()
    )
  end

  def select_agg_value(query, chart_aggregate, last_chart_field) do
    case chart_aggregate do
      :sum -> select_merge(query, [..., l], %{value: sum(field(l, ^last_chart_field))})
      :avg -> select_merge(query, [..., l], %{value: avg(field(l, ^last_chart_field))})
      :count -> select_merge(query, [..., l], %{value: count(field(l, ^last_chart_field))})
    end
  end

  def select_timestamp(query, chart_period) do
    case chart_period do
      :day ->
        select(query, [t, ...], %{
          timestamp: bq_timestamp_trunc(t.timestamp, "day")
        })

      :hour ->
        select(query, [t, ...], %{
          timestamp: bq_timestamp_trunc(t.timestamp, "hour")
        })

      :minute ->
        select(query, [t, ...], %{
          timestamp: bq_timestamp_trunc(t.timestamp, "minute")
        })

      :second ->
        select(query, [t, ...], %{
          timestamp: bq_timestamp_trunc(t.timestamp, "second")
        })
    end
  end

  def join_missing_range_timestamps(q, min, max, chart_period) do
    case chart_period do
      :day ->
        join(
          subquery(q),
          :full,
          [t, ...],
          ts in fragment(
            "(SELECT timestamp, 0 as value
            FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
              TIMESTAMP_TRUNC(?, DAY),
              TIMESTAMP_TRUNC(?, DAY),
              INTERVAL 1 DAY))
             AS timestamp)",
            ^min,
            ^max
          ),
          on: t.timestamp == ts.timestamp
        )

      :hour ->
        join(
          subquery(q),
          :full,
          [t, ...],
          ts in fragment(
            "(SELECT timestamp, 0 as value
            FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
              TIMESTAMP_TRUNC(?, HOUR),
              TIMESTAMP_TRUNC(?, HOUR),
              INTERVAL 1 HOUR))
             AS timestamp)",
            ^min,
            ^max
          ),
          on: t.timestamp == ts.timestamp
        )

      :minute ->
        join(
          subquery(q),
          :full,
          [t, ...],
          ts in fragment(
            "(SELECT timestamp, 0 as value
            FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
              TIMESTAMP_TRUNC(?, MINUTE),
              TIMESTAMP_TRUNC(?, MINUTE),
              INTERVAL 1 MINUTE))
             AS timestamp)",
            ^min,
            ^max
          ),
          on: t.timestamp == ts.timestamp
        )

      :second ->
        join(
          subquery(q),
          :full,
          [t, ...],
          ts in fragment(
            "(SELECT timestamp, 0 as value
            FROM UNNEST(GENERATE_TIMESTAMP_ARRAY(
              TIMESTAMP_TRUNC(?, SECOND),
              TIMESTAMP_TRUNC(?, SECOND),
              INTERVAL 1 SECOND))
             AS timestamp)",
            ^min,
            ^max
          ),
          on: t.timestamp == ts.timestamp
        )
    end
  end
end
