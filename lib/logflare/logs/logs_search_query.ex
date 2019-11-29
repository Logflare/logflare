defmodule Logflare.Logs.Search.Query do
  @moduledoc false
  import Ecto.Query

  def limit_aggregate_chart_period(query, search_chart_period)
      when search_chart_period in ~w(day hour minute second)a do
    case search_chart_period do
      :day -> limit(query, 30)
      :hour -> limit(query, 168)
      :minute -> limit(query, 120)
      :second -> limit(query, 180)
    end
  end

  def timestamp_truncator(search_chart_period) do
    case search_chart_period do
      :day -> dynamic([t], fragment("TIMESTAMP_TRUNC(?, DAY)", t.timestamp))
      :hour -> dynamic([t], fragment("TIMESTAMP_TRUNC(?, HOUR)", t.timestamp))
      :minute -> dynamic([t], fragment("TIMESTAMP_TRUNC(?, MINUTE)", t.timestamp))
      :second -> dynamic([t], fragment("TIMESTAMP_TRUNC(?, SECOND)", t.timestamp))
    end
  end

  def where_partitiondate(query, min, max) do
    where(
      query,
      [t, ...],
      fragment(
        "_PARTITIONDATE BETWEEN TIMESTAMP_TRUNC(?, DAY) AND TIMESTAMP_TRUNC(?, DAY)",
        ^min,
        ^max
      )
    )
  end

  def where_timestamp_tailing(query, min, max) do
    where(query, [t, ...], t.timestamp >= ^min and t.timestamp <= ^max)
  end

  def where_tailing_partitiondate(query, search_chart_period) do
    case search_chart_period do
      :day ->
        query
        |> where(
          [t, ...],
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 31 DAY) OR _PARTITIONDATE IS NULL"
          )
        )

      :hour ->
        query
        |> where(
          [t, ...],
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY) OR _PARTITIONDATE IS NULL"
          )
        )

      :minute ->
        query
        |> where(
          [t, ...],
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) OR _PARTITIONDATE IS NULL"
          )
        )

      :second ->
        query
        |> where(
          [t, ...],
          fragment(
            "_PARTITIONDATE >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) OR _PARTITIONDATE IS NULL"
          )
        )
    end
  end

  def select_agg_value(query, search_chart_aggregate, last_chart_field) do
    case search_chart_aggregate do
      :sum -> select_merge(query, [..., l], %{value: sum(field(l, ^last_chart_field))})
      :avg -> select_merge(query, [..., l], %{value: avg(field(l, ^last_chart_field))})
      :count -> select_merge(query, [..., l], %{value: count(field(l, ^last_chart_field))})
    end
  end

  def select_timestamp(query, search_chart_period) do
    case search_chart_period do
      :day ->
        select(query, [t, ...], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, DAY)", t.timestamp)
        })

      :hour ->
        select(query, [t, ...], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, HOUR)", t.timestamp)
        })

      :minute ->
        select(query, [t, ...], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, MINUTE)", t.timestamp)
        })

      :second ->
        select(query, [t, ...], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, SECOND)", t.timestamp)
        })
    end
  end

  def join_missing_range_timestamps(q, min, max, search_chart_period) do
    case search_chart_period do
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
