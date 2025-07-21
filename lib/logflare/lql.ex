defmodule Logflare.Lql do
  @moduledoc """
  The main LQL (Logflare Query Language) module.

  This module provides the primary API for parsing, encoding, and decoding LQL queries.
  It acts as a backend-agnostic interface while maintaining backward compatibility
  with BigQuery-specific functions.
  """

  import Logflare.Utils.Guards

  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias __MODULE__.BackendTransformer
  alias __MODULE__.Encoder
  alias __MODULE__.Parser

  @doc """
  Parses LQL query string with schema validation.

  This function accepts any schema format but maintains backward compatibility
  with BigQuery TableSchema for existing usage.
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
  with BigQuery TableSchema for existing usage.
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
  Delete when all source rules are migrated to LQL.

  This function maintains backward compatibility with the old API.
  """
  @spec build_message_filter_from_regex(regex :: String.t()) :: {:ok, [term()]} | {:error, term()}
  def build_message_filter_from_regex(regex) when is_non_empty_binary(regex) do
    Parser.parse(regex, SchemaBuilder.initial_table_schema())
  end

  @doc """
  Applies filter rules to a query using the appropriate transformer.

  This is the main public API for applying LQL filters to queries.
  """
  @spec apply_filter_rules_to_query(Ecto.Query.t(), [term()], keyword()) :: Ecto.Query.t()
  def apply_filter_rules_to_query(query, filter_rules, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.apply_filter_rules_to_query(query, filter_rules, opts)
  end

  @doc """
  Handles nested field access using the appropriate transformer.

  This function routes to the backend-specific nested field handling.
  """
  @spec handle_nested_field_access(Ecto.Query.t(), String.t(), keyword()) :: Ecto.Query.t()
  def handle_nested_field_access(query, path, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.handle_nested_field_access(query, path)
  end

  @doc """
  Creates a dynamic where clause using the appropriate transformer.

  This function routes to the backend-specific filter rule transformation.
  """
  @spec transform_filter_rule(term(), keyword()) :: Ecto.Query.DynamicExpr.t()
  def transform_filter_rule(filter_rule, opts \\ []) do
    adapter = Keyword.get(opts, :adapter, :bigquery)
    transformer = BackendTransformer.for_dialect(adapter)
    transformer.transform_filter_rule(filter_rule, %{})
  end

  @doc """
  Checks if modifiers indicate negation.

  This is a utility function that works across all backends.
  """
  @spec is_negated?(map()) :: boolean()
  def is_negated?(modifiers), do: Map.get(modifiers, :negate, false)
end
