defmodule Logflare.Lql.BackendTransformer.BigQuery do
  @moduledoc """
  BigQuery-specific LQL backend transformer implementation.

  This module implements the `Logflare.Lql.BackendTransformer` behaviour for BigQuery, providing
  translation of LQL `FilterRule` and `ChartRule` structs into BigQuery-compatible
  Ecto queries with proper UNNEST operations for nested fields.
  """

  @behaviour Logflare.Lql.BackendTransformer

  import Ecto.Query

  alias Ecto.Query
  alias Ecto.Query.DynamicExpr

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
    # BigQuery uses the base transformation data as-is
    # Future implementations might add BigQuery-specific fields
    base_data
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

  @spec unnest_and_join_nested_columns(
          query :: Query.t(),
          join_type :: :inner | :left | :right | :full,
          path :: String.t()
        ) :: Query.t()
  defp unnest_and_join_nested_columns(query, join_type, path) do
    path
    |> split_by_dots()
    |> Enum.slice(0..-2//1)
    |> case do
      [] ->
        query

      columns ->
        columns
        |> Enum.with_index(1)
        |> Enum.reduce(query, fn {column, level}, q ->
          column = String.to_atom(column)

          if level === 1 do
            join(q, join_type, [top], n in fragment("UNNEST(?)", field(top, ^column)), on: true)
          else
            join(q, join_type, [..., n1], n in fragment("UNNEST(?)", field(n1, ^column)),
              on: true
            )
          end
        end)
    end
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
        ) :: DynamicExpr.t()
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

    if is_negated?(modifiers) do
      dynamic([..., n1], not (^clause))
    else
      clause
    end
  end

  @spec is_negated?(map()) :: boolean()
  defp is_negated?(modifiers), do: Map.get(modifiers, :negate)

  @spec split_by_dots(String.t()) :: [String.t()]
  defp split_by_dots(value) when is_binary(value) do
    value
    |> String.split(".")
    |> List.wrap()
  end

  @doc """
  Query filter where `timestamp` is older than a given interval.

  `unit` must be one of `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, or `DAY`.

  ## Examples

      iex> from("logs") |> where_timestamp_ago(~U[2025-02-21 03:27:12Z], 1, "MINUTE")
      #Ecto.Query<from l0 in "logs", where: l0.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MINUTE)", ^~U[2025-02-21 03:27:12Z], ^1)>

      iex> from("logs") |> where_timestamp_ago(~U[2025-02-21 03:27:12Z], 1, "ILLEGAL_VALUE")
      ** (ArgumentError) Invalid interval: ILLEGAL_VALUE
  """
  @spec where_timestamp_ago(Ecto.Query.t(), DateTime.t(), integer(), String.t()) :: Ecto.Query.t()
  def where_timestamp_ago(query, datetime, count, unit) do
    case unit do
      "MICROSECOND" -> query |> where([t], t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MICROSECOND)", ^datetime, ^count))
      "MILLISECOND" -> query |> where([t], t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MILLISECOND)", ^datetime, ^count))
      "SECOND" -> query |> where([t], t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? SECOND)", ^datetime, ^count))
      "MINUTE" -> query |> where([t], t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? MINUTE)", ^datetime, ^count))
      "HOUR" -> query |> where([t], t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? HOUR)", ^datetime, ^count))
      "DAY" -> query |> where([t], t.timestamp >= fragment("TIMESTAMP_SUB(?, INTERVAL ? DAY)", ^datetime, ^count))
      _ -> raise ArgumentError, "Invalid interval: #{unit}"
    end
  end
end
