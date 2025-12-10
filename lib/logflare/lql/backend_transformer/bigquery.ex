defmodule Logflare.Lql.BackendTransformer.BigQuery do
  @moduledoc """
  BigQuery-specific LQL backend transformer implementation.

  This module implements the `Logflare.Lql.BackendTransformer` behaviour for BigQuery, providing
  translation of LQL `ChartRule`, `FilterRule`, and `SelectRule` structs into BigQuery-compatible
  Ecto queries with proper UNNEST operations for nested fields.
  """

  @behaviour Logflare.Lql.BackendTransformer

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Ecto.Query
  alias Logflare.Lql.Rules.ChartRule

  @special_top_level ~w(_PARTITIONDATE _PARTITIONTIME event_message timestamp id)

  # macros used for generating Ecto query fragments
  defmacrop bq_trunc_second(ts_field) do
    quote do: fragment("TIMESTAMP_TRUNC(?, SECOND)", unquote(ts_field))
  end

  defmacrop bq_trunc_minute(ts_field) do
    quote do: fragment("TIMESTAMP_TRUNC(?, MINUTE)", unquote(ts_field))
  end

  defmacrop bq_trunc_hour(ts_field) do
    quote do: fragment("TIMESTAMP_TRUNC(?, HOUR)", unquote(ts_field))
  end

  defmacrop bq_trunc_day(ts_field) do
    quote do: fragment("TIMESTAMP_TRUNC(?, DAY)", unquote(ts_field))
  end

  @impl true
  def dialect, do: "bigquery"

  @impl true
  def quote_style, do: "`"

  @impl true
  # BigQuery doesn't require specific validation beyond the common schema
  # Future implementations might validate BigQuery-specific fields
  def validate_transformation_data(%{schema: _}), do: :ok

  def validate_transformation_data(_data) do
    {:error, "BigQuery transformer requires schema in transformation data"}
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
    {top_level_filters, other_filters} =
      Enum.split_with(
        rules,
        &(&1.path in @special_top_level or not String.contains?(&1.path, "."))
      )

    query =
      top_level_filters
      |> Enum.reduce(
        query,
        fn rule, qacc -> where_match_filter_rule(qacc, rule) end
      )

    other_filters
    |> Enum.reduce(
      query,
      fn rule, qacc ->
        qacc
        |> handle_nested_field_access(rule.path)
        |> where_match_filter_rule(rule)
      end
    )
  end

  @impl true
  def handle_nested_field_access(query, field_path) do
    unnest_and_join_nested_columns(query, :inner, field_path)
  end

  @impl true
  def transform_filter_rule(filter_rule, _transformation_data) do
    column =
      filter_rule.path
      |> split_by_dots()
      |> List.last()

    if not is_nil(filter_rule.values) and filter_rule.operator == :range do
      [lvalue, rvalue] = filter_rule.values
      dynamic([..., n1], fragment("? BETWEEN ? AND ?", field(n1, ^column), ^lvalue, ^rvalue))
    else
      dynamic_where_filter_rule(
        column,
        filter_rule.operator,
        filter_rule.value,
        filter_rule.modifiers
      )
    end
  end

  @doc """
  Transforms a ChartRule into a BigQuery chart query with time-series aggregation.

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
      timestamp: bq_trunc_second(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], bq_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :countd, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_second(field(t, ^timestamp_field)),
      count: count(field(t, ^field_path), :distinct)
    })
    |> group_by([t], bq_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_minute(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :countd, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_minute(field(t, ^timestamp_field)),
      count: count(field(t, ^field_path), :distinct)
    })
    |> group_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_hour(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :countd, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_hour(field(t, ^timestamp_field)),
      count: count(field(t, ^field_path), :distinct)
    })
    |> group_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_day(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], bq_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :countd, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_day(field(t, ^timestamp_field)),
      count: count(field(t, ^field_path), :distinct)
    })
    |> group_by([t], bq_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_second(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_minute(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_hour(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_day(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_second(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_minute(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_hour(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_day(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :second, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_second(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_minute(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_hour(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: bq_trunc_day(field(t, ^timestamp_field)),
      count: max(field(t, ^field_path))
    })
    |> group_by([t], bq_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :second, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: bq_trunc_second(field(t, ^timestamp_field)),
      count:
        fragment(
          "APPROX_QUANTILES(?, 100)[OFFSET(?)]",
          field(t, ^field_path),
          ^trunc(percentile_value * 100)
        )
    })
    |> group_by([t], bq_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :minute, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: bq_trunc_minute(field(t, ^timestamp_field)),
      count:
        fragment(
          "APPROX_QUANTILES(?, 100)[OFFSET(?)]",
          field(t, ^field_path),
          ^trunc(percentile_value * 100)
        )
    })
    |> group_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :hour, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: bq_trunc_hour(field(t, ^timestamp_field)),
      count:
        fragment(
          "APPROX_QUANTILES(?, 100)[OFFSET(?)]",
          field(t, ^field_path),
          ^trunc(percentile_value * 100)
        )
    })
    |> group_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :day, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)

    query
    |> select([t], %{
      timestamp: bq_trunc_day(field(t, ^timestamp_field)),
      count:
        fragment(
          "APPROX_QUANTILES(?, 100)[OFFSET(?)]",
          field(t, ^field_path),
          ^trunc(percentile_value * 100)
        )
    })
    |> group_by([t], bq_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], bq_trunc_day(field(t, ^timestamp_field)))
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
      nested_columns = split_by_dots(path)
      {:nested_field, nested_columns, unnest_paths_for_select(nested_columns)}
    end
  end

  def transform_select_rule(select_rule, _transformation_data) do
    {:error, "Invalid SelectRule: #{inspect(select_rule)}"}
  end

  @doc """
  Query filter where `timestamp` is older than a given interval.

  `unit` must be one of `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, or `DAY`.
  """
  @spec where_timestamp_ago(Query.t(), DateTime.t(), integer(), String.t()) :: Query.t()
  def where_timestamp_ago(query, datetime, count, unit) do
    case unit do
      "MICROSECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MICROSECOND)", ^datetime, ^count)
        )

      "MILLISECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MILLISECOND)", ^datetime, ^count)
        )

      "SECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? SECOND)", ^datetime, ^count)
        )

      "MINUTE" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MINUTE)", ^datetime, ^count)
        )

      "HOUR" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? HOUR)", ^datetime, ^count)
        )

      "DAY" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? DAY)", ^datetime, ^count)
        )

      _ ->
        raise ArgumentError, "Invalid interval: #{unit}"
    end
  end

  @spec unnest_and_join_nested_columns(
          query :: Query.t(),
          join_type :: :inner | :left | :right | :full,
          path :: String.t()
        ) :: Query.t()
  defp unnest_and_join_nested_columns(query, join_type, path) do
    path
    |> split_by_dots()
    |> Enum.slice(0..-2//1)
    |> Enum.with_index(1)
    |> Enum.reduce(query, fn {column, level}, acc_query ->
      add_unnest_join(acc_query, join_type, column, level)
    end)
  end

  defp add_unnest_join(query, join_type, column, 1) do
    join(query, join_type, [top], n in fragment("UNNEST(?)", field(top, ^column)), on: true)
  end

  defp add_unnest_join(query, join_type, column, _level) do
    join(query, join_type, [..., n1], n in fragment("UNNEST(?)", field(n1, ^column)), on: true)
  end

  @spec where_match_filter_rule(
          query :: Query.t(),
          rule :: map()
        ) :: Query.t()
  defp where_match_filter_rule(query, rule) do
    column =
      rule.path
      |> split_by_dots()
      |> List.last()

    if not is_nil(rule.values) and rule.operator == :range do
      [lvalue, rvalue] = rule.values
      where(query, [..., n1], fragment("? BETWEEN ? AND ?", field(n1, ^column), ^lvalue, ^rvalue))
    else
      where(query, ^dynamic_where_filter_rule(column, rule.operator, rule.value, rule.modifiers))
    end
  end

  @spec dynamic_where_filter_rule(
          column :: String.t(),
          operator :: atom(),
          value :: any(),
          modifiers :: map()
        ) :: Query.dynamic_expr()
  defp dynamic_where_filter_rule(column, operator, value, modifiers) do
    clause =
      case operator do
        :> ->
          dynamic([..., n1], field(n1, ^column) > ^value)

        :>= ->
          dynamic([..., n1], field(n1, ^column) >= ^value)

        :< ->
          dynamic([..., n1], field(n1, ^column) < ^value)

        :<= ->
          dynamic([..., n1], field(n1, ^column) <= ^value)

        := ->
          case value do
            :NULL -> dynamic([..., n1], fragment(~s|? IS NULL|, field(n1, ^column)))
            _ -> dynamic([..., n1], field(n1, ^column) == ^value)
          end

        :"~" ->
          dynamic([..., n1], fragment(~s|REGEXP_CONTAINS(?, ?)|, field(n1, ^column), ^value))

        :string_contains ->
          dynamic([..., n1], fragment(~s|STRPOS(?, ?) > 0|, field(n1, ^column), ^value))

        :list_includes ->
          dynamic([..., n1], fragment(~s|? IN UNNEST(?)|, ^value, field(n1, ^column)))

        :list_includes_regexp ->
          dynamic(
            [..., n1],
            fragment(
              ~s|EXISTS(SELECT * FROM UNNEST(?) AS x WHERE REGEXP_CONTAINS(x, ?))|,
              field(n1, ^column),
              ^value
            )
          )
      end

    if negated?(modifiers) do
      case {operator, value} do
        {:=, :NULL} -> dynamic([..., n1], not (^clause))
        {_, _} -> dynamic([..., n1], is_nil(field(n1, ^column)) or not (^clause))
      end
    else
      clause
    end
  end

  @spec negated?(map()) :: boolean()
  defp negated?(modifiers), do: Map.get(modifiers, :negate)

  @spec build_combined_select(Query.t(), [Logflare.Lql.Rules.SelectRule.t()]) :: Query.t()
  defp build_combined_select(query, select_rules) do
    Enum.reduce(select_rules, query, fn %{path: path, alias: alias}, acc_query ->
      is_nested = path not in @special_top_level and String.contains?(path, ".")
      field_name = path |> split_by_dots() |> List.last()
      add_select_for_field(acc_query, path, field_name, alias, is_nested)
    end)
  end

  @spec add_select_for_field(Query.t(), String.t(), String.t(), String.t() | nil, boolean()) ::
          Query.t()
  defp add_select_for_field(query, path, field_name, alias, true = _is_nested) do
    name = if is_binary(alias), do: alias, else: String.replace(path, ".", "_")

    query
    |> handle_nested_field_access(path)
    |> select_merge([..., t], %{
      ^name => fragment("? AS ?", field(t, ^field_name), identifier(^name))
    })
  end

  defp add_select_for_field(query, _path, field_name, alias, false = _is_nested)
       when is_binary(alias) do
    select_merge(query, [t], %{
      ^alias => fragment("? AS ?", field(t, ^field_name), identifier(^alias))
    })
  end

  defp add_select_for_field(query, path, field_name, nil = _alias, false = _is_nested) do
    select_merge(query, [t], %{^path => field(t, ^field_name)})
  end

  defp unnest_paths_for_select(nested_columns) when length(nested_columns) <= 1, do: []

  defp unnest_paths_for_select(nested_columns) do
    nested_columns
    |> Enum.with_index()
    |> Enum.take(length(nested_columns) - 1)
    |> Enum.map(fn {_column, index} ->
      nested_columns
      |> Enum.take(index + 1)
      |> Enum.join(".")
    end)
  end

  @spec split_by_dots(String.t()) :: [String.t()]
  defp split_by_dots(value) when is_binary(value) do
    value
    |> String.split(".")
    |> List.wrap()
  end
end
