defmodule Logflare.Lql.BackendTransformer.BigQuery do
  @moduledoc """
  BigQuery-specific LQL backend transformer implementation.

  This module implements the `Logflare.Lql.BackendTransformer` behaviour for BigQuery, providing
  translation of LQL `ChartRule`, `FilterRule`, and `SelectRule` structs into BigQuery-compatible
  Ecto queries with proper UNNEST operations for nested fields.
  """

  @behaviour Logflare.Lql.BackendTransformer

  import Ecto.Query

  alias Ecto.Query

  @special_top_level ~w(_PARTITIONDATE _PARTITIONTIME event_message timestamp id)

  @impl true
  def dialect, do: "bigquery"

  @impl true
  def quote_style, do: "`"

  @impl true
  def validate_transformation_data(data) do
    # BigQuery doesn't require specific validation beyond the common schema
    # Future implementations might validate BigQuery-specific fields
    case data do
      %{schema: _} -> :ok
      _ -> {:error, "BigQuery transformer requires schema in transformation data"}
    end
  end

  @impl true
  def build_transformation_data(base_data) do
    # currently using the base transformation data as-is
    base_data
  end

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
      |> String.to_atom()

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

  @impl true
  def transform_chart_rule(_chart_rule, _transformation_data) do
    # TODO: Implement chart rule transformation for BigQuery
    # This would handle aggregations, grouping, etc.
    raise "Chart rule transformation not yet implemented for BigQuery transformer"
  end

  @impl true
  def transform_select_rule(%{wildcard: true}, _transformation_data) do
    {:wildcard, []}
  end

  def transform_select_rule(%{path: path} = _select_rule, _transformation_data)
      when is_binary(path) do
    if path in @special_top_level or not String.contains?(path, ".") do
      {:field, String.to_atom(path), []}
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
      add_unnest_join(acc_query, join_type, String.to_atom(column), level)
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
      |> String.to_atom()

    if not is_nil(rule.values) and rule.operator == :range do
      [lvalue, rvalue] = rule.values
      where(query, [..., n1], fragment("? BETWEEN ? AND ?", field(n1, ^column), ^lvalue, ^rvalue))
    else
      where(query, ^dynamic_where_filter_rule(column, rule.operator, rule.value, rule.modifiers))
    end
  end

  @spec dynamic_where_filter_rule(
          column :: atom(),
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

  @spec build_combined_select(Query.t(), [map()]) :: Query.t()
  defp build_combined_select(query, select_rules) do
    Enum.reduce(select_rules, query, fn %{path: path}, acc_query ->
      field_atom =
        if path in @special_top_level or not String.contains?(path, ".") do
          String.to_atom(path)
        else
          String.replace(path, ".", "_") |> String.to_atom()
        end

      select_merge(acc_query, [l], %{^field_atom => field(l, ^field_atom)})
    end)
  end

  @spec unnest_paths_for_select([String.t()]) :: [String.t()]
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
