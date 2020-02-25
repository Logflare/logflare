defmodule Logflare.Logs.SearchQueries do
  @moduledoc false
  import Ecto.Query
  @chart_periods ~w(day hour minute second)a
  alias Logflare.Ecto.BQQueryAPI
  alias Logflare.Lql
  import BQQueryAPI.UDF
  import BQQueryAPI

  def select_aggregates(q, chart_period) do
    q
    |> select([t], %{
      timestamp:
        fragment(
          "(`$$__DEFAULT_DATASET__$$`.LF_TIMESTAMP_TRUNC(?, ?)) as timestamp",
          t.timestamp,
          ^String.upcase("#{chart_period}")
        )
    })
  end

  def select_aggregates(q) do
    q
    |> select([t, ts], %{
      timestamp: fragment("? as timestamp", coalesce(t.timestamp, ts.timestamp)),
      datetime: fragment("DATETIME(?) AS datetime", coalesce(t.timestamp, ts.timestamp)),
      value: fragment("? as value", coalesce(t.value, ts.value))
    })
  end

  @spec select_merge_agg_value(any, :avg | :count | :sum, any) :: Ecto.Query.t()
  def select_merge_agg_value(query, chart_aggregate, last_chart_field) do
    case chart_aggregate do
      :sum -> select_merge(query, [..., l], %{value: sum(field(l, ^last_chart_field))})
      :avg -> select_merge(query, [..., l], %{value: avg(field(l, ^last_chart_field))})
      :count -> select_merge(query, [..., l], %{value: count(field(l, ^last_chart_field))})
    end
  end

  def select_merge_log_count(query) do
    query
    |> select_merge([l, ...], %{value: fragment("COUNT(?) as value", l.timestamp)})
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

  def select_timestamp_trunc(query, chart_period) do
    select(query, [t, ...], %{
      timestamp: lf_timestamp_trunc(t.timestamp, ^chart_period)
    })
  end

  def select_and_group_by_http_status_code(q) do
    q
    |> Lql.EctoHelpers.unnest_and_join_nested_columns(:left, "metadata.response.status_code")
    |> select_merge([..., t], %{
      other:
        fragment(
          "COUNTIF(? <= 99 OR ? >= 501 OR ? IS NULL) as other",
          t.status_code,
          t.status_code,
          t.status_code
        ),
      status_1xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_1xx", t.status_code, 100, 199),
      status_2xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_2xx", t.status_code, 200, 299),
      status_3xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_3xx", t.status_code, 300, 399),
      status_4xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_4xx", t.status_code, 400, 499),
      status_5xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_5xx", t.status_code, 500, 599)
    })
    |> group_by(1)
  end

  def select_and_group_by_log_level(q) do
    q
    |> Lql.EctoHelpers.unnest_and_join_nested_columns(:left, "metadata.level")
    |> select_merge([..., t], %{
      other:
        fragment(
          "COUNTIF(? NOT IN UNNEST(?) OR ? IS NULL) as other",
          t.level,
          [
            "debug",
            "info",
            "warn",
            "error"
          ],
          t.level
        ),
      level_debug: fragment("COUNTIF(? = ?) as level_debug", t.level, "debug"),
      level_info: fragment("COUNTIF(? = ?) as level_info", t.level, "info"),
      level_warn: fragment("COUNTIF(? = ?) as level_warn", t.level, "warn"),
      level_error: fragment("COUNTIF(? = ?) as level_error", t.level, "error")
    })
    |> group_by(1)
  end
end
