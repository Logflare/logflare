defmodule Logflare.Logs.SearchQueries do
  @moduledoc false

  import Ecto.Query
  import Logflare.Ecto.BQQueryAPI

  alias Logflare.Lql

  @chart_periods ~w(day hour minute second)a

  def select_timestamp(query, chart_period) do
    query
    |> select([t], %{
      timestamp:
        fragment(
          """
          (case
          when ? = 'DAY'  then TIMESTAMP_TRUNC(?, DAY)
          when ? = 'HOUR'  then TIMESTAMP_TRUNC(?, HOUR)
          when ? = 'MINUTE'  then TIMESTAMP_TRUNC(?, MINUTE)
          when ? = 'SECOND'  then TIMESTAMP_TRUNC(?, SECOND)
          end) as timestamp
          """,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^String.upcase("#{chart_period}"),
          t.timestamp
        )
    })
  end

  def select_timestamp(query, chart_period, timezone) do
    query
    |> select([t], %{
      timestamp:
        fragment(
          """
          (case
          when ? = 'DAY'  then TIMESTAMP_TRUNC(?, DAY , ?)
          when ? = 'HOUR'  then TIMESTAMP_TRUNC(?, HOUR , ?)
          when ? = 'MINUTE'  then TIMESTAMP_TRUNC(?, MINUTE , ?)
          when ? = 'SECOND'  then TIMESTAMP_TRUNC(?, SECOND , ?)
          end) as timestamp
          """,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^timezone,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^timezone,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^timezone,
          ^String.upcase("#{chart_period}"),
          t.timestamp,
          ^timezone
        )
    })
  end

  @spec select_merge_agg_value(any, :avg | :count | :sum | :max, atom()) :: Ecto.Query.t()
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

      :max ->
        select_merge(query, [..., l], %{
          value: fragment("MAX(?) as value", field(l, ^last_chart_field))
        })

      :p50 ->
        select_merge(query, [..., l], %{
          value:
            fragment("APPROX_QUANTILES(?, 100)[OFFSET(50)] as value", field(l, ^last_chart_field))
        })

      :p95 ->
        select_merge(query, [..., l], %{
          value:
            fragment("APPROX_QUANTILES(?, 100)[OFFSET(95)] as value", field(l, ^last_chart_field))
        })

      :p99 ->
        select_merge(query, [..., l], %{
          value:
            fragment("APPROX_QUANTILES(?, 100)[OFFSET(99)] as value", field(l, ^last_chart_field))
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

  def select_merge_total(query) do
    select_merge(query, [t, ...], %{
      total: fragment("COUNT(?) as total", t.timestamp)
    })
  end

  def select_count_netlify_http_status_code(query) do
    query
    |> Lql.handle_nested_field_access("metadata.status_code")
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

  def select_count_cloudflare_http_status_code(query) do
    query
    |> Lql.handle_nested_field_access("metadata.response.status_code")
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

  def select_count_vercel_http_status_code(query) do
    query
    |> Lql.handle_nested_field_access("metadata.proxy.statusCode")
    |> select_merge([..., t], %{
      other:
        fragment(
          "COUNTIF(? <= 99 OR ? >= 601 OR ? IS NULL) as other",
          t.statusCode,
          t.statusCode,
          t.statusCode
        ),
      status_1xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_1xx", t.statusCode, 100, 199),
      status_2xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_2xx", t.statusCode, 200, 299),
      status_3xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_3xx", t.statusCode, 300, 399),
      status_4xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_4xx", t.statusCode, 400, 499),
      status_5xx: fragment("COUNTIF(? BETWEEN ? AND ?) as status_5xx", t.statusCode, 500, 599)
    })
    |> select_merge_total()
  end

  def select_count_log_level(query) do
    query
    |> Lql.handle_nested_field_access("metadata.level")
    |> select_merge([..., t], %{
      other:
        fragment(
          "COUNTIF(LOWER(?) NOT IN UNNEST(?) OR ? IS NULL) as other",
          t.level,
          [
            "debug",
            "info",
            "warn",
            "warning",
            "error",
            "notice",
            "critical",
            "alert",
            "emergency"
          ],
          t.level
        ),
      level_notice: fragment("COUNTIF(lower(?) = ?) as level_notice", t.level, "notice"),
      level_critical: fragment("COUNTIF(lower(?) = ?) as level_critical", t.level, "critical"),
      level_alert: fragment("COUNTIF(lower(?) = ?) as level_alert", t.level, "alert"),
      level_emergency: fragment("COUNTIF(lower(?) = ?) as level_emergency", t.level, "emergency"),
      level_debug: fragment("COUNTIF(lower(?) = ?) as level_debug", t.level, "debug"),
      level_info: fragment("COUNTIF(lower(?) = ?) as level_info", t.level, "info"),
      # FIXME
      level_warn:
        fragment(
          "COUNTIF(lower(?) = ? OR lower(?) = ?) as level_warn",
          t.level,
          "warn",
          t.level,
          "warning"
        ),
      level_error: fragment("COUNTIF(lower(?) = ?) as level_error", t.level, "error")
    })
    |> select_merge_total()
  end

  def source_log_event_query(bq_table_id, id, timestamp) when is_binary(id) do
    query =
      from(bq_table_id)
      |> where([t], t.id == ^id)
      |> or_where([t], t.timestamp == ^timestamp)
      |> select([t], fragment("*"))

    le_date = Timex.to_date(timestamp)

    if timestamp.hour >= 22 do
      le_date_plus_1 = Timex.shift(le_date, days: 1)

      where(
        query,
        partition_date() == ^le_date or partition_date() == ^le_date_plus_1 or
          in_streaming_buffer()
      )
    else
      where(query, partition_date() == ^le_date or in_streaming_buffer())
    end
  end

  def source_log_event_by_path(bq_table_id, path, value)
      when is_binary(bq_table_id) and is_binary(path) do
    last_column = String.split(path, ".") |> List.last() |> String.to_atom()

    from(bq_table_id)
    |> select([t], %{
      t
      | id: t.id,
        timestamp: t.timestamp,
        message: t.event_message
    })
    |> Lql.handle_nested_field_access(path)
    |> where([..., t1], field(t1, ^last_column) == ^value)
  end

  def source_log_event_id(bq_table_id, id) when is_binary(bq_table_id) when is_binary(id) do
    from(bq_table_id)
    |> where([t], t.id == ^id)
    |> select([t], fragment("*"))
  end

  def source_table_streaming_buffer(bq_table_id) when is_binary(bq_table_id) do
    from(bq_table_id)
    |> select([t], fragment("*"))
    |> where(in_streaming_buffer())
  end

  def source_table_last_1_minutes(bq_table_id) when is_binary(bq_table_id) do
    from(bq_table_id)
    |> select([t], fragment("*"))
    |> where([t], t.timestamp >= ^Timex.shift(DateTime.utc_now(), seconds: -60))
  end

  def where_log_id(query, id) do
    where(query, [t], t.id == ^id)
  end
end
