defmodule Logflare.Lql.BackendTransformer.Postgres do
  @moduledoc """
  PostgreSQL-specific LQL backend transformer implementation.

  This module implements the `Logflare.Lql.BackendTransformer` behaviour for PostgreSQL,
  providing translation of LQL rules into PostgreSQL-compatible Ecto queries with proper
  JSONB path operations for nested field access.
  """

  @behaviour Logflare.Lql.BackendTransformer

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Ecto.Query
  alias Logflare.Lql.Rules.ChartRule

  @special_top_level ~w(event_message timestamp id)

  # PostgreSQL-specific macros for time truncation
  defmacrop pg_trunc_second(ts_field) do
    quote do: fragment("date_trunc('second', ?)", unquote(ts_field))
  end

  defmacrop pg_trunc_minute(ts_field) do
    quote do: fragment("date_trunc('minute', ?)", unquote(ts_field))
  end

  defmacrop pg_trunc_hour(ts_field) do
    quote do: fragment("date_trunc('hour', ?)", unquote(ts_field))
  end

  defmacrop pg_trunc_day(ts_field) do
    quote do: fragment("date_trunc('day', ?)", unquote(ts_field))
  end

  @impl true
  def dialect, do: "postgres"

  @impl true
  def quote_style, do: "\""

  @impl true
  def validate_transformation_data(%{schema: _}), do: :ok

  def validate_transformation_data(_data) do
    {:error, "Postgres transformer requires schema in transformation data"}
  end

  @impl true
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
    # PostgreSQL handles nested fields via JSONB operators without joins
    query
  end

  @impl true
  def transform_filter_rule(filter_rule, _transformation_data) do
    field_path = filter_rule.path

    if not is_nil(filter_rule.values) and filter_rule.operator == :range do
      [lvalue, rvalue] = filter_rule.values

      case build_jsonb_path_expression(field_path) do
        {:top_level, field} ->
          dynamic(
            [l],
            fragment("? BETWEEN ? AND ?", field(l, ^field), ^lvalue, ^rvalue)
          )

        {:jsonb, path_expr} ->
          dynamic(
            [l],
            fragment("(?)::numeric BETWEEN ? AND ?", ^path_expr, ^lvalue, ^rvalue)
          )
      end
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
  Transforms a `ChartRule` into a PostgreSQL chart query with time-series aggregation.

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
      timestamp: pg_trunc_second(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], pg_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :minute, timestamp_field) do
    query
    |> select([t], %{
      timestamp: pg_trunc_minute(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :hour, timestamp_field) do
    query
    |> select([t], %{
      timestamp: pg_trunc_hour(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :count, _field_path, :day, timestamp_field) do
    query
    |> select([t], %{
      timestamp: pg_trunc_day(field(t, ^timestamp_field)),
      count: count(field(t, ^timestamp_field))
    })
    |> group_by([t], pg_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :second, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_second(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :minute, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_minute(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :hour, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_hour(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :avg, field_path, :day, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_day(field(t, ^timestamp_field)),
      count: avg(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :second, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_second(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :minute, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_minute(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :hour, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_hour(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :sum, field_path, :day, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_day(field(t, ^timestamp_field)),
      count: sum(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :second, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_second(field(t, ^timestamp_field)),
      count: max(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :minute, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_minute(field(t, ^timestamp_field)),
      count: max(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :hour, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_hour(field(t, ^timestamp_field)),
      count: max(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, :max, field_path, :day, timestamp_field) do
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_day(field(t, ^timestamp_field)),
      count: max(field(t, ^field_name))
    })
    |> group_by([t], pg_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_day(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :second, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_second(field(t, ^timestamp_field)),
      count:
        fragment(
          "percentile_cont(?) WITHIN GROUP (ORDER BY ?)",
          ^percentile_value,
          field(t, ^field_name)
        )
    })
    |> group_by([t], pg_trunc_second(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_second(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :minute, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_minute(field(t, ^timestamp_field)),
      count:
        fragment(
          "percentile_cont(?) WITHIN GROUP (ORDER BY ?)",
          ^percentile_value,
          field(t, ^field_name)
        )
    })
    |> group_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_minute(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :hour, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_hour(field(t, ^timestamp_field)),
      count:
        fragment(
          "percentile_cont(?) WITHIN GROUP (ORDER BY ?)",
          ^percentile_value,
          field(t, ^field_name)
        )
    })
    |> group_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_hour(field(t, ^timestamp_field)))
  end

  def transform_chart_rule(query, percentile, field_path, :day, timestamp_field)
      when is_percentile_aggregate(percentile) do
    percentile_value = ChartRule.percentile_to_value(percentile)
    field_name = get_field_name_for_aggregation(field_path)

    query
    |> select([t], %{
      timestamp: pg_trunc_day(field(t, ^timestamp_field)),
      count:
        fragment(
          "percentile_cont(?) WITHIN GROUP (ORDER BY ?)",
          ^percentile_value,
          field(t, ^field_name)
        )
    })
    |> group_by([t], pg_trunc_day(field(t, ^timestamp_field)))
    |> order_by([t], pg_trunc_day(field(t, ^timestamp_field)))
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
          t.timestamp >= fragment("? - INTERVAL '? microseconds'", ^datetime, ^count)
        )

      "MILLISECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("? - INTERVAL '? milliseconds'", ^datetime, ^count)
        )

      "SECOND" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("? - INTERVAL '? seconds'", ^datetime, ^count)
        )

      "MINUTE" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("? - INTERVAL '? minutes'", ^datetime, ^count)
        )

      "HOUR" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("? - INTERVAL '? hours'", ^datetime, ^count)
        )

      "DAY" ->
        query
        |> where(
          [t],
          t.timestamp >= fragment("? - INTERVAL '? days'", ^datetime, ^count)
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

      case build_jsonb_path_expression(field_path) do
        {:top_level, field} ->
          where(query, [l], fragment("? BETWEEN ? AND ?", field(l, ^field), ^lvalue, ^rvalue))

        {:jsonb, path_expr} ->
          where(
            query,
            [l],
            fragment("(?)::numeric BETWEEN ? AND ?", ^path_expr, ^lvalue, ^rvalue)
          )
      end
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
      case build_jsonb_path_expression(field_path) do
        {:top_level, field} ->
          build_operator_clause_top_level(field, operator, value)

        {:jsonb, path_expr} ->
          build_operator_clause_jsonb(path_expr, operator, value)
      end

    apply_negation_if_needed(clause, field_path, operator, value, modifiers)
  end

  @spec apply_negation_if_needed(
          Query.dynamic_expr(),
          String.t(),
          atom(),
          any(),
          map()
        ) :: Query.dynamic_expr()
  defp apply_negation_if_needed(clause, _field_path, :=, :NULL, %{negate: true}) do
    dynamic([l], not (^clause))
  end

  defp apply_negation_if_needed(clause, field_path, _operator, _value, %{negate: true}) do
    case build_jsonb_path_expression(field_path) do
      {:top_level, field} ->
        dynamic([l], fragment("? IS NULL", field(l, ^field)) or not (^clause))

      {:jsonb, _path_expr} ->
        dynamic([l], not (^clause))
    end
  end

  defp apply_negation_if_needed(clause, _field_path, _operator, _value, _modifiers) do
    clause
  end

  @spec build_operator_clause_top_level(String.t(), atom(), any()) :: Query.dynamic_expr()
  defp build_operator_clause_top_level(field, operator, value) do
    case operator do
      :> ->
        dynamic([l], field(l, ^field) > ^value)

      :>= ->
        dynamic([l], field(l, ^field) >= ^value)

      :< ->
        dynamic([l], field(l, ^field) < ^value)

      :<= ->
        dynamic([l], field(l, ^field) <= ^value)

      := ->
        case value do
          :NULL -> dynamic([l], fragment("? IS NULL", field(l, ^field)))
          _ -> dynamic([l], field(l, ^field) == ^value)
        end

      :"~" ->
        dynamic([l], fragment("? ~ ?", field(l, ^field), ^value))

      :string_contains ->
        dynamic([l], fragment("? LIKE ?", field(l, ^field), ^"%#{value}%"))

      :list_includes ->
        dynamic([l], fragment("? @> ?::jsonb", field(l, ^field), ^Jason.encode!([value])))

      :list_includes_regexp ->
        dynamic(
          [l],
          fragment(
            "EXISTS(SELECT 1 FROM jsonb_array_elements_text(?) AS x WHERE x ~ ?)",
            field(l, ^field),
            ^value
          )
        )
    end
  end

  @spec build_operator_clause_jsonb(String.t(), atom(), any()) :: Query.dynamic_expr()
  defp build_operator_clause_jsonb(path_expr, operator, value) do
    case operator do
      :> ->
        dynamic([l], fragment("(?)::numeric > ?", ^path_expr, ^value))

      :>= ->
        dynamic([l], fragment("(?)::numeric >= ?", ^path_expr, ^value))

      :< ->
        dynamic([l], fragment("(?)::numeric < ?", ^path_expr, ^value))

      :<= ->
        dynamic([l], fragment("(?)::numeric <= ?", ^path_expr, ^value))

      := ->
        case value do
          :NULL -> dynamic([l], fragment("? IS NULL", ^path_expr))
          _ -> dynamic([l], fragment("? = ?", ^path_expr, ^value))
        end

      :"~" ->
        dynamic([l], fragment("? ~ ?", ^path_expr, ^value))

      :string_contains ->
        dynamic([l], fragment("? LIKE ?", ^path_expr, ^"%#{value}%"))

      :list_includes ->
        # For JSONB arrays, use @> operator
        path_expr_array = String.replace(path_expr, "->>", "->")
        dynamic([l], fragment("? @> ?::jsonb", ^path_expr_array, ^Jason.encode!([value])))

      :list_includes_regexp ->
        path_expr_array = String.replace(path_expr, "->>", "->")

        dynamic(
          [l],
          fragment(
            "EXISTS(SELECT 1 FROM jsonb_array_elements_text(?) AS x WHERE x ~ ?)",
            ^path_expr_array,
            ^value
          )
        )
    end
  end

  @spec build_jsonb_path_expression(String.t()) :: {:top_level, String.t()} | {:jsonb, String.t()}
  defp build_jsonb_path_expression(field_path) do
    case String.split(field_path, ".") do
      [field] when field in @special_top_level ->
        {:top_level, field}

      ["m" | rest] ->
        # m.status -> body->'metadata'->>'status'
        # m.user.email -> body->'metadata'->'user'->>'email'
        path = ["metadata" | rest]
        {:jsonb, build_jsonb_accessor(path)}

      [field] ->
        # Single field not in special list - might be a direct JSONB field
        {:top_level, field}

      path ->
        # Nested path without 'm.' prefix
        {:jsonb, build_jsonb_accessor(path)}
    end
  end

  @spec build_jsonb_accessor([String.t()]) :: String.t()
  defp build_jsonb_accessor(path) when is_list(path) and length(path) > 0 do
    # For ["metadata", "status"]: body->'metadata'->>'status'
    # For ["metadata", "user", "email"]: body->'metadata'->'user'->>'email'
    {last, rest} = List.pop_at(path, -1)

    middle =
      rest
      |> Enum.map(fn key -> "->'#{key}'" end)
      |> Enum.join()

    "body#{middle}->>'#{last}'"
  end

  defp get_field_name_for_aggregation(field_path) do
    case build_jsonb_path_expression(field_path) do
      {:top_level, field} -> field
      {:jsonb, _path_expr} -> String.replace(field_path, ".", "_")
    end
  end

  @spec build_combined_select(Query.t(), select_rules :: [map()]) :: Query.t()
  defp build_combined_select(query, select_rules) do
    Enum.reduce(select_rules, query, fn %{path: path}, acc_query ->
      select_merge(acc_query, [l], %{^path => field(l, ^path)})
    end)
  end
end
