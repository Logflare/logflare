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
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Ecto.SqlUtils

  @default_dialect :bigquery

  @type dialect :: :bigquery | :clickhouse | :postgres
  @type period :: :second | :minute | :hour | :day
  @type sql_language :: :bq_sql | :ch_sql | :pg_sql

  @doc """
  Converts a language identifier to a dialect identifier.
  """
  @spec language_to_dialect(sql_language()) :: dialect()
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
        # Wildcard or no select rules - just select everything
        from(t in cte_table_name, select: t)
      else
        # Build select with explicit fields as a map
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
          chart_rule :: term(),
          filter_rules :: [term()],
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

    case dialect do
      :bigquery ->
        build_bigquery_chart_query(query, aggregate, path, period, "timestamp")

      :clickhouse ->
        build_clickhouse_chart_query(query, aggregate, path, period, "timestamp")
    end
  end

  @spec build_bigquery_chart_query(
          query :: Ecto.Query.t(),
          aggregate :: atom(),
          field_path :: String.t(),
          period :: period(),
          timestamp_field :: String.t()
        ) ::
          Ecto.Query.t()
  defp build_bigquery_chart_query(query, aggregate, field_path, period, timestamp_field) do
    case aggregate do
      :count ->
        query
        |> select([t], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period),
          count: count(field(t, ^timestamp_field))
        })
        |> group_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))
        |> order_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))

      :avg ->
        query
        |> select([t], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period),
          count: avg(field(t, ^field_path))
        })
        |> group_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))
        |> order_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))

      :sum ->
        query
        |> select([t], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period),
          count: sum(field(t, ^field_path))
        })
        |> group_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))
        |> order_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))

      :max ->
        query
        |> select([t], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period),
          count: max(field(t, ^field_path))
        })
        |> group_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))
        |> order_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))

      percentile when percentile in [:p50, :p95, :p99] ->
        percentile_value =
          case percentile do
            :p50 -> 0.5
            :p95 -> 0.95
            :p99 -> 0.99
          end

        query
        |> select([t], %{
          timestamp: fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period),
          count:
            fragment(
              "APPROX_QUANTILES(?, 100)[OFFSET(?)]",
              field(t, ^field_path),
              ^trunc(percentile_value * 100)
            )
        })
        |> group_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))
        |> order_by([t], fragment("TIMESTAMP_TRUNC(?, ?)", field(t, ^timestamp_field), ^period))
    end
  end

  @spec build_clickhouse_chart_query(
          query :: Ecto.Query.t(),
          aggregate :: atom(),
          field_path :: String.t(),
          period :: period(),
          timestamp_field :: String.t()
        ) ::
          Ecto.Query.t()
  defp build_clickhouse_chart_query(query, aggregate, field_path, period, timestamp_field) do
    interval = period_to_clickhouse_interval(period)

    case aggregate do
      :count ->
        query
        |> select([t], %{
          timestamp:
            fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval),
          count: count(field(t, ^timestamp_field))
        })
        |> group_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )
        |> order_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )

      :avg ->
        query
        |> select([t], %{
          timestamp:
            fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval),
          count: avg(field(t, ^field_path))
        })
        |> group_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )
        |> order_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )

      :sum ->
        query
        |> select([t], %{
          timestamp:
            fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval),
          count: sum(field(t, ^field_path))
        })
        |> group_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )
        |> order_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )

      :max ->
        query
        |> select([t], %{
          timestamp:
            fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval),
          count: max(field(t, ^field_path))
        })
        |> group_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )
        |> order_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )

      percentile when percentile in [:p50, :p95, :p99] ->
        percentile_value =
          case percentile do
            :p50 -> 0.5
            :p95 -> 0.95
            :p99 -> 0.99
          end

        query
        |> select([t], %{
          timestamp:
            fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval),
          count: fragment("quantile(?)(?))", ^percentile_value, field(t, ^field_path))
        })
        |> group_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )
        |> order_by(
          [t],
          fragment("toStartOfInterval(?, INTERVAL 1 ?)", field(t, ^timestamp_field), ^interval)
        )
    end
  end

  @spec period_to_clickhouse_interval(period()) :: String.t()
  defp period_to_clickhouse_interval(:second), do: "second"
  defp period_to_clickhouse_interval(:minute), do: "minute"
  defp period_to_clickhouse_interval(:hour), do: "hour"
  defp period_to_clickhouse_interval(:day), do: "day"

  @spec ecto_query_to_sql_string(Ecto.Query.t(), :bigquery | :clickhouse) ::
          {:ok, String.t()} | {:error, String.t()}
  defp ecto_query_to_sql_string(query, :bigquery) do
    with {:ok, {pg_sql, _pg_params}} <- SqlUtils.ecto_to_pg_sql(query) do
      # Convert PostgreSQL SQL to BigQuery SQL format
      bq_sql =
        pg_sql
        # Remove PostgreSQL-style parameter placeholders ($1, $2, etc.)
        |> String.replace(~r/\$\d+/, "?")
        # Remove quotes around column names (BigQuery doesn't need them for simple identifiers)
        |> String.replace(~r/\."([\w\.]+)"/, ".\\1")
        # Remove quotes around table names
        |> String.replace(~r/FROM\s+"(.+?)"/, "FROM \\1")
        # Remove quotes around aliases
        |> String.replace(~r/AS\s+"(\w+)"/, "AS \\1")

      {:ok, bq_sql}
    end
  end

  defp ecto_query_to_sql_string(query, :clickhouse) do
    with {:ok, {ch_sql, _ch_params}} <- ClickhouseAdaptor.ecto_to_sql(query, []) do
      {:ok, ch_sql}
    end
  end
end
