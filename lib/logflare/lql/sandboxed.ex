defmodule Logflare.Lql.Sandboxed do
  @moduledoc """
  Sandboxed LQL query functions.
  """

  import Ecto.Query
  import Logflare.Utils.Guards

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Lql.BackendTransformer
  alias Logflare.Lql.Parser
  alias Logflare.Lql.Rules
  alias Logflare.Lql.Rules.ChartRule
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.FromRule
  alias Logflare.Lql.Rules.SelectRule

  @typep dialect :: :bigquery | :clickhouse | :postgres

  @doc """
  Converts an LQL query string to a SQL query string for use in sandboxed endpoints.

  If the LQL string contains a `FromRule` (e.g., `f:table_name`), that table name
  will be used instead of the `cte_table_name` parameter.
  """
  @spec to_sandboxed_sql(
          lql_string :: String.t(),
          cte_table_name :: String.t(),
          dialect :: dialect()
        ) :: {:ok, String.t()} | {:error, String.t()}
  def to_sandboxed_sql(lql_string, cte_table_name, dialect)
      when is_binary(lql_string) and is_non_empty_binary(cte_table_name) and
             dialect in [:bigquery, :clickhouse, :postgres] do
    with {:ok, lql_rules} <- Parser.parse(lql_string) do
      # Determine table name: use FromRule if present, otherwise use cte_table_name parameter
      table_name =
        case Rules.get_from_rule(lql_rules) do
          %FromRule{table: table} -> table
          nil -> cte_table_name
        end

      filter_rules = Rules.get_filter_rules(lql_rules)
      chart_rule = Rules.get_chart_rule(lql_rules)

      query =
        if chart_rule do
          build_chart_query(table_name, chart_rule, filter_rules, dialect)
        else
          select_rules = Rules.get_select_rules(lql_rules)

          build_select_query(table_name, select_rules, filter_rules, dialect)
        end

      ecto_query_to_sql_string(query, dialect)
    end
  end

  @spec build_select_query(
          cte_table_name :: String.t(),
          select_rules :: [SelectRule.t()],
          filter_rules :: [FilterRule.t()],
          dialect :: dialect()
        ) :: Ecto.Query.t()
  defp build_select_query(cte_table_name, select_rules, filter_rules, dialect) do
    query = build_select_clause(cte_table_name, select_rules, filter_rules)
    Logflare.Lql.apply_filter_rules(query, filter_rules, dialect: dialect)
  end

  @spec build_select_clause(
          cte_table_name :: String.t(),
          select_rules :: [SelectRule.t()],
          filter_rules :: [FilterRule.t()]
        ) :: Ecto.Query.t()
  defp build_select_clause(cte_table_name, [], filter_rules) do
    build_inferred_select(cte_table_name, filter_rules)
  end

  defp build_select_clause(cte_table_name, select_rules, filter_rules) do
    if Rules.has_wildcard_selection?(select_rules) do
      build_inferred_select(cte_table_name, filter_rules)
    else
      build_explicit_select(cte_table_name, select_rules)
    end
  end

  @spec build_inferred_select(cte_table_name :: String.t(), filter_rules :: [FilterRule.t()]) ::
          Ecto.Query.t()
  defp build_inferred_select(cte_table_name, []) do
    from(t in cte_table_name, select: %{timestamp: field(t, :timestamp)})
  end

  defp build_inferred_select(cte_table_name, filter_rules) do
    inferred_fields = infer_select_fields_from_filters(filter_rules)

    select_map =
      Enum.reduce(inferred_fields, %{}, fn path, acc ->
        Map.put(acc, path, dynamic([t], field(t, ^path)))
      end)

    from(t in cte_table_name, select: ^select_map)
  end

  @spec build_explicit_select(cte_table_name :: String.t(), select_rules :: [SelectRule.t()]) ::
          Ecto.Query.t()
  defp build_explicit_select(cte_table_name, select_rules) do
    select_map =
      Enum.reduce(select_rules, %{}, fn %{path: path}, acc ->
        Map.put(acc, path, dynamic([t], field(t, ^path)))
      end)

    from(t in cte_table_name, select: ^select_map)
  end

  @spec build_chart_query(
          cte_table_name :: String.t(),
          chart_rule :: ChartRule.t(),
          filter_rules :: [FilterRule.t()],
          dialect :: dialect()
        ) :: Ecto.Query.t()
  defp build_chart_query(
         cte_table_name,
         %ChartRule{aggregate: aggregate, path: path, period: period},
         filter_rules,
         dialect
       ) do
    query =
      from(t in cte_table_name)
      |> Logflare.Lql.apply_filter_rules(filter_rules, dialect: dialect)

    transformer = BackendTransformer.for_dialect(dialect)
    transformer.transform_chart_rule(query, aggregate, path, period, "timestamp")
  end

  @spec infer_select_fields_from_filters(filter_rules :: [FilterRule.t()]) :: [String.t()]
  defp infer_select_fields_from_filters(filter_rules) do
    filter_rules
    |> Enum.map(fn %FilterRule{path: path} -> path end)
    |> Enum.uniq()
  end

  @spec ecto_query_to_sql_string(query :: Ecto.Query.t(), dialect :: dialect()) ::
          {:ok, String.t()} | {:error, String.t()}
  defp ecto_query_to_sql_string(query, dialect) do
    adaptor =
      case dialect do
        :bigquery -> BigQueryAdaptor
        :clickhouse -> ClickhouseAdaptor
        :postgres -> PostgresAdaptor
      end

    with {:ok, {sql, _params}} <- adaptor.ecto_to_sql(query, inline_params: true) do
      {:ok, sql}
    end
  end
end
