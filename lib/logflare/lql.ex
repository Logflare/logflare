defmodule Logflare.Lql do
  @moduledoc """
  The main LQL (Logflare Query Language) module.

  This module provides the primary API for parsing, encoding, and decoding LQL queries.
  It acts as a backend-agnostic interface while maintaining backward compatibility
  with BigQuery-specific functions.
  """

  import Ecto.Query

  alias __MODULE__.BackendTransformer
  alias __MODULE__.Encoder
  alias __MODULE__.Parser
  alias __MODULE__.Rules
  alias __MODULE__.Rules.ChartRule
  alias __MODULE__.Rules.FilterRule
  alias __MODULE__.Rules.SelectRule
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor

  @default_dialect :bigquery

  @typep dialect :: :bigquery | :clickhouse | :postgres

  @doc """
  Converts a language identifier to a dialect identifier.
  """
  @spec language_to_dialect(:bq_sql | :ch_sql | :pg_sql) :: dialect()
  def language_to_dialect(:bq_sql), do: :bigquery
  def language_to_dialect(:ch_sql), do: :clickhouse
  def language_to_dialect(_), do: :bigquery

  @doc """
  Parses LQL query string with schema validation.

  This function accepts any schema format but maintains backward compatibility
  with BigQuery `TableSchema` for existing usage.
  """
  @spec decode(qs :: String.t(), table_schema :: TS.t()) :: {:ok, [term()]} | {:error, term()}
  def decode(qs, %TS{} = table_schema) when is_binary(qs) do
    Parser.parse(qs, table_schema)
  end

  @spec decode(qs :: String.t(), schema :: any()) :: {:ok, [term()]} | {:error, term()}
  def decode(qs, schema) when is_binary(qs) do
    Parser.parse(qs, schema)
  end

  @doc """
  Parses LQL query string with schema validation, raising on error.

  This function accepts any schema format but maintains backward compatibility
  with BigQuery `TableSchema` for existing usage.
  """
  @spec decode!(qs :: String.t(), table_schema :: TS.t()) :: [term()]
  def decode!(qs, %TS{} = table_schema) when is_binary(qs) do
    {:ok, lql_rules} = Parser.parse(qs, table_schema)
    lql_rules
  end

  @spec decode!(qs :: String.t(), schema :: any()) :: [term()]
  def decode!(qs, schema) when is_binary(qs) do
    {:ok, lql_rules} = Parser.parse(qs, schema)
    lql_rules
  end

  @doc """
  Encodes LQL rules back to query string.

  This function is backend-agnostic and works with any LQL rules.
  """
  @spec encode(lql_rules :: [term()]) :: {:ok, String.t()}
  def encode(lql_rules) when is_list(lql_rules) do
    {:ok, Encoder.to_querystring(lql_rules)}
  end

  @doc """
  Encodes LQL rules back to query string, raising on error.

  This function is backend-agnostic and works with any LQL rules.
  """
  @spec encode!(lql_rules :: [term()]) :: String.t()
  def encode!(lql_rules) do
    Encoder.to_querystring(lql_rules)
  end

  @doc """
  Applies all LQL rules to a query using the appropriate transformer.

  This is a convenience function that applies filter rules, select rules, and any other
  applicable transformations in the correct order.
  """
  @spec apply_rules(query :: Ecto.Query.t(), lql_rules :: [term()], opts :: Keyword.t()) ::
          Ecto.Query.t()
  def apply_rules(query, lql_rules, opts \\ []) do
    filter_rules = Rules.get_filter_rules(lql_rules)
    select_rules = Rules.get_select_rules(lql_rules)

    query
    |> apply_filter_rules(filter_rules, opts)
    |> apply_select_rules(select_rules, opts)
  end

  @doc """
  Applies filter rules to a query using the appropriate transformer.
  """
  @spec apply_filter_rules(
          query :: Ecto.Query.t(),
          filter_rules :: [FilterRule.t()],
          opts :: Keyword.t()
        ) ::
          Ecto.Query.t()
  def apply_filter_rules(query, filter_rules, opts \\ []) do
    dialect = Keyword.get(opts, :dialect, @default_dialect)
    transformer = BackendTransformer.for_dialect(dialect)
    transformer.apply_filter_rules_to_query(query, filter_rules, opts)
  end

  @doc """
  Applies select rules to a query using the appropriate transformer.
  """
  @spec apply_select_rules(
          query :: Ecto.Query.t(),
          select_rules :: [SelectRule.t()],
          opts :: Keyword.t()
        ) ::
          Ecto.Query.t()
  def apply_select_rules(query, select_rules, opts \\ []) do
    dialect = Keyword.get(opts, :dialect, @default_dialect)
    transformer = BackendTransformer.for_dialect(dialect)
    transformer.apply_select_rules_to_query(query, select_rules, opts)
  end

  @doc """
  Handles nested field access using the appropriate backend transformer.
  """
  @spec handle_nested_field_access(
          query :: Ecto.Query.t(),
          path :: String.t(),
          opts :: Keyword.t()
        ) :: Ecto.Query.t()
  def handle_nested_field_access(query, path, opts \\ []) do
    dialect = Keyword.get(opts, :dialect, @default_dialect)
    transformer = BackendTransformer.for_dialect(dialect)
    transformer.handle_nested_field_access(query, path)
  end

  @doc """
  Creates a dynamic where clause using the appropriate backend transformer.
  """
  @spec transform_filter_rule(filter_rule :: FilterRule.t(), opts :: Keyword.t()) ::
          Ecto.Query.dynamic_expr()
  def transform_filter_rule(filter_rule, opts \\ []) do
    dialect = Keyword.get(opts, :dialect, @default_dialect)
    transformer = BackendTransformer.for_dialect(dialect)
    transformer.transform_filter_rule(filter_rule, %{})
  end

  @doc """
  Converts an LQL query string to a SQL query string for use in sandboxed endpoints.

  This function is designed for sandboxed query contexts where LQL needs to be converted
  to SQL that operates on CTE tables. The generated SQL will select from the specified
  `cte_table_name`.
  """
  @spec to_sandboxed_sql(
          lql_string :: String.t(),
          cte_table_name :: String.t(),
          dialect :: dialect()
        ) :: {:ok, String.t()} | {:error, String.t()}
  def to_sandboxed_sql(lql_string, cte_table_name, dialect)
      when is_binary(lql_string) and is_binary(cte_table_name) and
             dialect in [:bigquery, :clickhouse] do
    with {:ok, lql_rules} <- Parser.parse(lql_string) do
      filter_rules = Rules.get_filter_rules(lql_rules)
      chart_rule = Rules.get_chart_rule(lql_rules)

      query =
        if chart_rule do
          build_chart_query(cte_table_name, chart_rule, filter_rules, dialect)
        else
          select_rules = Rules.get_select_rules(lql_rules)

          build_select_query(cte_table_name, select_rules, filter_rules, dialect)
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
    query =
      if Enum.empty?(select_rules) or Rules.has_wildcard_selection?(select_rules) do
        from(t in cte_table_name, select: fragment("*"))
      else
        select_map =
          Enum.reduce(select_rules, %{}, fn %{path: path}, acc ->
            Map.put(acc, path, dynamic([t], field(t, ^path)))
          end)

        from(t in cte_table_name, select: ^select_map)
      end

    apply_filter_rules(query, filter_rules, dialect: dialect)
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
      |> apply_filter_rules(filter_rules, dialect: dialect)

    transformer = BackendTransformer.for_dialect(dialect)
    transformer.transform_chart_rule(query, aggregate, path, period, "timestamp")
  end

  @spec ecto_query_to_sql_string(query :: Ecto.Query.t(), dialect :: dialect()) ::
          {:ok, String.t()} | {:error, String.t()}
  defp ecto_query_to_sql_string(query, dialect) do
    adaptor =
      case dialect do
        :bigquery -> BigQueryAdaptor
        :clickhouse -> ClickhouseAdaptor
      end

    with {:ok, {sql, _params}} <- adaptor.ecto_to_sql(query, inline_params: true) do
      {:ok, sql}
    end
  end
end
