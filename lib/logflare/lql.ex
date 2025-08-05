defmodule Logflare.Lql do
  @moduledoc """
  The main LQL (Logflare Query Language) module.

  This module provides the primary API for parsing, encoding, and decoding LQL queries.
  It acts as a backend-agnostic interface while maintaining backward compatibility
  with BigQuery-specific functions.
  """

  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias __MODULE__.BackendTransformer
  alias __MODULE__.Encoder
  alias __MODULE__.Parser
  alias __MODULE__.Rules

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
  @spec apply_rules(Ecto.Query.t(), [term()], keyword()) :: Ecto.Query.t()
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
  @spec apply_filter_rules(Ecto.Query.t(), [term()], keyword()) :: Ecto.Query.t()
  def apply_filter_rules(query, filter_rules, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.apply_filter_rules_to_query(query, filter_rules, opts)
  end

  @doc """
  Applies select rules to a query using the appropriate transformer.
  """
  @spec apply_select_rules(Ecto.Query.t(), [term()], keyword()) :: Ecto.Query.t()
  def apply_select_rules(query, select_rules, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.apply_select_rules_to_query(query, select_rules, opts)
  end

  @doc """
  Handles nested field access using the appropriate backend transformer.
  """
  @spec handle_nested_field_access(Ecto.Query.t(), String.t(), keyword()) :: Ecto.Query.t()
  def handle_nested_field_access(query, path, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.handle_nested_field_access(query, path)
  end

  @doc """
  Creates a dynamic where clause using the appropriate backend transformer.
  """
  @spec transform_filter_rule(term(), keyword()) :: Ecto.Query.DynamicExpr.t()
  def transform_filter_rule(filter_rule, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.transform_filter_rule(filter_rule, %{})
  end
end
