defmodule Logflare.Logs.SearchQueries do
  @moduledoc false
  import Ecto.Query
  @chart_periods ~w(day hour minute second)a
  alias Logflare.Ecto.BQQueryAPI
  alias Logflare.Lql
  import BQQueryAPI.UDF
  import BQQueryAPI

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

  def timestamp_truncator(period) when period in @chart_periods do
    dynamic([t], lf_timestamp_trunc(t.timestamp, ^period))
  end

  def where_streaming_buffer(query) do
    where(query, in_streaming_buffer())
  end

  def where_partitiondate_between(query, min, max) do
    where(
      query,
      [t, ...],
      fragment(
        "_PARTITIONDATE BETWEEN DATE_TRUNC(?, DAY) AND DATE_TRUNC(?, DAY)",
        ^Timex.to_date(min),
        ^Timex.to_date(max)
      )
    )
  end

  @spec select_agg_value(any, :avg | :count | :sum, any) :: Ecto.Query.t()
  def select_agg_value(query, chart_aggregate, last_chart_field) do
    case chart_aggregate do
      :sum -> select_merge(query, [..., l], %{value: sum(field(l, ^last_chart_field))})
      :avg -> select_merge(query, [..., l], %{value: avg(field(l, ^last_chart_field))})
      :count -> select_merge(query, [..., l], %{value: count(field(l, ^last_chart_field))})
    end
  end

  def select_log_count(query) do
    query
    |> select_merge([l, ...], %{value: count(l.timestamp)})
  end

  def select_timestamp_trunc(query, chart_period) do
    select(query, [t, ...], %{
      timestamp: lf_timestamp_trunc(t.timestamp, ^chart_period)
    })
  end

  def join_missing_range_timestamps(q, min, max, chart_period) do
    join(
      subquery(q),
      :full,
      [t, ...],
      ts in fragment(
        "(SELECT timestamp, 0 as value
            FROM UNNEST(`$$__DEFAULT_DATASET__$$`.LF_GENERATE_TIMESTAMP_ARRAY(
              `$$__DEFAULT_DATASET__$$`.LF_TIMESTAMP_TRUNC(?, ?),
              `$$__DEFAULT_DATASET__$$`.LF_TIMESTAMP_TRUNC(?, ?),
              ?,
              ?
              ))
             AS timestamp)",
        ^min,
        ^BQQueryAPI.to_bq_interval_token(chart_period),
        ^max,
        ^BQQueryAPI.to_bq_interval_token(chart_period),
        1,
        ^BQQueryAPI.to_bq_interval_token(chart_period)
      ),
      on: t.timestamp == ts.timestamp
    )
  end
end
