defmodule Logflare.Logs.SearchQueries do
  @moduledoc false
  import Ecto.Query
  @chart_periods ~w(day hour minute second)a
  alias Logflare.Ecto.BQQueryAPI
  alias Logflare.Lql
  import BQQueryAPI.UDF
  import BQQueryAPI

  def select_timestamp(q, chart_period) do
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

  @spec select_merge_agg_value(any, :avg | :count | :sum, atom()) :: Ecto.Query.t()
  def select_merge_agg_value(query, :count, :timestamp) do
    select_merge(query, [t, ...], %{
      value: fragment("COUNT(?) as value", t.timestamp)
    })
  end

  def select_merge_agg_value(query, chart_aggregate, last_chart_field) do
    case chart_aggregate do
      :sum ->
        select_merge(query, [..., l], %{
          value: fragment("SUM(?) as value", field(l, ^last_chart_field))
        })

      :avg ->
        select_merge(query, [..., l], %{
          value: fragment("AVG(?) as value", field(l, ^last_chart_field))
        })

      :count ->
        select_merge(query, [..., l], %{
          value: fragment("COUNT(?) as value", field(l, ^last_chart_field))
        })
    end
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

  def select_merge_total(query) do
    select_merge(query, [t, ...], %{
      total: fragment("COUNT(?) as total", t.timestamp)
    })
  end

  def select_count_http_status_code(q) do
    q
    |> Lql.EctoHelpers.unnest_and_join_nested_columns(:inner, "metadata.response.status_code")
    |> select_merge([..., t], %{
      other:
        fragment(
          "COUNTIF(? <= 99 OR ? >= 601 OR ? IS NULL) as other",
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
    |> select_merge_total()
  end

  def select_count_log_level(q) do
    q
    |> Lql.EctoHelpers.unnest_and_join_nested_columns(:inner, "metadata.level")
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
    |> select_merge_total()
  end

  def source_log_event_query(bq_table_id, id, timestamp) when is_binary(id) do
    q =
      from(bq_table_id)
      |> where([t], t.id == ^id)
      |> or_where([t], t.timestamp == ^timestamp)
      |> select([t], %{
        metadata: t.metadata,
        id: t.id,
        timestamp: t.timestamp,
        message: t.event_message
      })

    le_date = Timex.to_date(timestamp)

    if timestamp.hour >= 22 do
      le_date_plus_1 = Timex.shift(le_date, days: 1)

      where(
        q,
        partition_date() == ^le_date or partition_date() == ^le_date_plus_1 or
          in_streaming_buffer()
      )
    else
      where(q, partition_date() == ^le_date or in_streaming_buffer())
    end
  end

  def select_default_fields(query, :events) do
    select(query, [:timestamp, :id, :event_message])
  end

  def source_table_streaming_buffer(bq_table_id) do
    from(bq_table_id)
    |> select([:id, :metadata, :timestamp, :event_message])
    |> where(in_streaming_buffer())
  end
end
