defmodule Logflare.Lql.BackendTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific LQL backend transformer implementation.

  This module implements the `Logflare.Lql.BackendTransformer` behaviour for ClickHouse, providing
  translation of LQL `ChartRule`, `FilterRule`, and `SelectRule` structs into ClickHouse-compatible
  Ecto queries with proper array operations and nested field access.
  """

  @behaviour Logflare.Lql.BackendTransformer

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Ecto.Query
  alias Logflare.Lql.Rules.ChartRule

  @special_top_level ~w(event_message timestamp id)

  # macros used for generating Ecto query fragments
  defmacrop ch_interval_second(ts_field) do
    quote do: fragment("toStartOfInterval(?, INTERVAL 1 second)", unquote(ts_field))
  end

  defmacrop ch_interval_minute(ts_field) do
    quote do: fragment("toStartOfInterval(?, INTERVAL 1 minute)", unquote(ts_field))
  end

  defmacrop ch_interval_hour(ts_field) do
    quote do: fragment("toStartOfInterval(?, INTERVAL 1 hour)", unquote(ts_field))
  end

  defmacrop ch_interval_day(ts_field) do
    quote do: fragment("toStartOfInterval(?, INTERVAL 1 day)", unquote(ts_field))
  end

  @impl true
  def dialect, do: "clickhouse"

  @impl true
  def quote_style, do: "\""

  @impl true
  def validate_transformation_data(%{schema: _}), do: :ok

  def validate_transformation_data(_data) do
    {:error, "ClickHouse transformer requires schema in transformation data"}
  end

  @impl true
  # currently using the base transformation data as-is
  def build_transformation_data(base_data), do: base_data

  @impl true
  def apply_select_rules_to_query(query, select_rules, _opts \\ [])
  def apply_select_rules_to_query(query, [], _opts), do: query

  def apply_select_rules_to_query(query, select_rules, _opts) do
    normalized_rules =
      case Enum.find(select_rules, & &1.wildcard) do
        nil -> select_rules
        _wildcard -> [%{wildcard: true, path: "*"}]
      end

    case normalized_rules do
      [] -> query
      [%{wildcard: true}] -> query
      rules -> build_combined_select(query, rules)
    end
  end

  @impl true
  def apply_filter_rules_to_query(query, filter_rules, opts \\ [])
  def apply_filter_rules_to_query(query, [], _opts), do: query

  def apply_filter_rules_to_query(query, rules, _opts) do
    Enum.reduce(rules, query, fn rule, acc_query ->
      where_match_filter_rule(acc_query, rule)
    end)
  end

  @impl true
  def handle_nested_field_access(query, _field_path) do
    # ClickHouse handles nested fields natively without joins
    query
  end

  @impl true
  def transform_filter_rule(filter_rule, _transformation_data) do
    field_path = filter_rule.path

    if not is_nil(filter_rule.values) and filter_rule.operator == :range do
      [lvalue, rvalue] = filter_rule.values

      dynamic(
        [l],
        fragment("? BETWEEN ? AND ?", field(l, ^field_path), ^lvalue, ^rvalue)
      )
    else
      dynamic_where_filter_rule(
        field_path,
        filter_rule.operator,
        filter_rule.value,
        filter_rule.modifiers
      )
    end
  end

  @doc """
  Transforms a ChartRule into a ClickHouse chart query with time-series aggregation.

  This function creates an Ecto query with appropriate GROUP BY, aggregation,
  and time truncation for the specified period and aggregate function.
  """
  @impl true
  @spec transform_chart_rule(
          query :: Ecto.Query.t(),
          aggregate :: atom(),
          field_path :: String.t(),
          period :: :second | :minute | :hour | :day,
          timestamp_field :: String.t()
        ) :: Ecto.Query.t()
  def transform_chart_rule(query, :count, _field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_second(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], ch_interval_second(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_minute(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], ch_interval_minute(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_hour(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], ch_interval_hour(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_day(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], ch_interval_day(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_second(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_second(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_minute(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_minute(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_hour(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_hour(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_day(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_day(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_second(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_second(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_minute(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_minute(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_hour(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_hour(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_day(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_day(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_second(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_second(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_minute(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_minute(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_hour(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_hour(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: ch_interval_day(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], ch_interval_day(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :second, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: ch_interval_second(field(t, ^timestamp_field)),
      count: fragment("quantile(?)(?))", ^percentile_value, field(t, ^field_path))
    })
    |> group_by([t], ch_interval_second(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :minute, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: ch_interval_minute(field(t, ^timestamp_field)),
      count: fragment("quantile(?)(?))", ^percentile_value, field(t, ^field_path))
    })
    |> group_by([t], ch_interval_minute(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :hour, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: ch_interval_hour(field(t, ^timestamp_field)),
      count: fragment("quantile(?)(?))", ^percentile_value, field(t, ^field_path))
    })
    |> group_by([t], ch_interval_hour(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :day, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: ch_interval_day(field(t, ^timestamp_field)),
      count: fragment("quantile(?)(?))", ^percentile_value, field(t, ^field_path))
    })
    |> group_by([t], ch_interval_day(field(t, ^timestamp_field)))
    |> order_by([t], ch_interval_day(field(t, ^timestamp_field)))
  end

  @impl true
  def transform_select_rule(%{wildcard: true}, _transformation_data) do
    {:wildcard, []}
  end

  def transform_select_rule(%{path: path} = _select_rule, _transformation_data)
      when is_binary(path) do
    if path in @special_top_level or not String.contains?(path, ".") do
      {:field, path, []}
    else
      {:nested_field, path, []}
    end
  end

  def transform_select_rule(select_rule, _transformation_data) do
    {:error, "Invalid SelectRule: #{inspect(select_rule)}"}
  end

  @doc """
  Query filter where `timestamp` is older than a given interval.

  `unit` must be one of `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, or `DAY`.
  """
  @spec where_timestamp_ago(Query.t(), DateTime.t(), count :: integer(), unit :: String.t()) ::
          Query.t()
  def where_timestamp_ago(query, datetime, count, unit) do
    case unit do
      "MICROSECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("subtractMicroseconds(?, ?)", ^datetime, ^count)
        )

      "MILLISECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("subtractMilliseconds(?, ?)", ^datetime, ^count)
        )

      "SECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("subtractSeconds(?, ?)", ^datetime, ^count)
        )

      "MINUTE" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("subtractMinutes(?, ?)", ^datetime, ^count)
        )

      "HOUR" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("subtractHours(?, ?)", ^datetime, ^count)
        )

      "DAY" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("subtractDays(?, ?)", ^datetime, ^count)
        )

      _ ->
        raise ArgumentError, "Invalid interval: #{unit}"
    end
  end

  @spec where_match_filter_rule(
          query :: Query.t(),
          rule :: map()
        ) :: Query.t()
  defp where_match_filter_rule(query, rule) do
    if not is_nil(rule.values) and rule.operator == :range do
      [lvalue, rvalue] = rule.values
      field_path = rule.path
      where(query, [l], fragment("? BETWEEN ? AND ?", field(l, ^field_path), ^lvalue, ^rvalue))
    else
      where(
        query,
        ^dynamic_where_filter_rule(rule.path, rule.operator, rule.value, rule.modifiers)
      )
    end
  end

  @spec dynamic_where_filter_rule(
          field_path :: String.t(),
          operator :: atom(),
          value :: any(),
          modifiers :: map()
        ) :: Query.dynamic_expr()
  defp dynamic_where_filter_rule(field_path, operator, value, modifiers) do
    clause =
      case operator do
        :> ->
          dynamic([l], field(l, ^field_path) > ^value)

        :>= ->
          dynamic([l], field(l, ^field_path) >= ^value)

        :< ->
          dynamic([l], field(l, ^field_path) < ^value)

        :<= ->
          dynamic([l], field(l, ^field_path) <= ^value)

        := ->
          case value do
            :NULL -> dynamic([l], fragment("? IS NULL", field(l, ^field_path)))
            _ -> dynamic([l], field(l, ^field_path) == ^value)
          end

        :"~" ->
          # ClickHouse uses match() function for regex
          dynamic([l], fragment("match(?, ?)", field(l, ^field_path), ^value))

        :string_contains ->
          # ClickHouse uses position() function for string search
          dynamic([l], fragment("position(?, ?) > 0", field(l, ^field_path), ^value))

        :list_includes ->
          # ClickHouse uses has() function for array membership
          dynamic([l], fragment("has(?, ?)", field(l, ^field_path), ^value))

        :list_includes_regexp ->
          # ClickHouse uses arrayExists with lambda for regex matching in arrays
          dynamic(
            [l],
            fragment("arrayExists(x -> match(x, ?), ?)", ^value, field(l, ^field_path))
          )
      end

    if negated?(modifiers) do
      case {operator, value} do
        {:=, :NULL} -> dynamic([l], not (^clause))
        {_, _} -> dynamic([l], fragment("? IS NULL", field(l, ^field_path)) or not (^clause))
      end
    else
      clause
    end
  end

  @spec negated?(map()) :: boolean()
  defp negated?(modifiers), do: Map.get(modifiers, :negate)

  @spec build_combined_select(Query.t(), select_rules :: [map()]) :: Query.t()
  defp build_combined_select(query, select_rules) do
    Enum.reduce(select_rules, query, fn %{path: path}, acc_query ->
      select_merge(acc_query, [l], %{^path => field(l, ^path)})
    end)
  end
end
