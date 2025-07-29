defmodule Logflare.Lql.BackendTransformer do
  @moduledoc """
  Behaviour for LQL-to-backend query transformations.

  This module defines the interface that all LQL backend transformer implementations must follow.
  Each backend transformer (BigQuery, ClickHouse, PostgreSQL, etc.) implements this behaviour
  to provide backend-specific query translation from LQL FilterRules and ChartRules.
  """

  alias Logflare.Lql.ChartRule
  alias Logflare.Lql.FilterRule
  alias Logflare.Sources.Source

  @type transformation_data :: %{
          sources: [Source.t()],
          source_mapping: %{String.t() => Source.t()},
          schema: any()
        }

  @type operators ::
          :<
          | :<=
          | :=
          | :>
          | :>=
          | :"~"
          | :list_includes
          | :list_includes_regexp
          | :string_contains
          | :range

  @type dialects :: :bigquery

  @doc """
  Transforms a single LQL FilterRule into a backend-specific query fragment.

  This is the core function that converts LQL filtering logic into the
  appropriate query structure for the backend (Ecto dynamic query, raw SQL, etc.).
  """
  @callback transform_filter_rule(FilterRule.t(), transformation_data()) :: term()

  @doc """
  Transforms a single LQL ChartRule into a backend-specific aggregation query.

  Converts LQL chart/aggregation rules into backend-specific query structures.
  """
  @callback transform_chart_rule(ChartRule.t(), transformation_data()) :: term()

  @doc """
  Applies multiple LQL FilterRules to a query, returning the modified query.

  This is the main entry point for applying LQL filters to a backend query.
  """
  @callback apply_filter_rules_to_query(query :: term(), [FilterRule.t()], keyword()) :: term()

  @doc """
  Returns the dialect string used by this transformer.

  Should return values like "bigquery", "clickhouse", "postgres", etc.
  """
  @callback dialect() :: String.t()

  @doc """
  Returns the quote style used by this backend for identifiers.

  Returns the character(s) used to quote table/column names, or nil if no quoting.
  """
  @callback quote_style() :: String.t() | nil

  @doc """
  Validates that the transformation data contains all required fields for this backend.

  Each transformer can define its own validation requirements.
  """
  @callback validate_transformation_data(transformation_data()) :: :ok | {:error, String.t()}

  @doc """
  Builds backend-specific transformation data from common inputs.

  This allows transformers to add their own required fields to the transformation data.
  """
  @callback build_transformation_data(base_data :: map()) :: transformation_data()

  @doc """
  Handles backend-specific nested field access (e.g., UNNEST operations).

  This function should handle accessing nested fields in the backend's native format.
  """
  @callback handle_nested_field_access(query :: term(), field_path :: String.t()) :: term()

  @doc """
  Returns the transformer module for a given dialect (atom or string).
  """
  @spec for_dialect(dialects() | String.t()) :: module()
  def for_dialect("bigquery"), do: __MODULE__.BigQuery
  def for_dialect(value) when is_atom(value), do: value |> to_dialect() |> for_dialect()

  @doc """
  Converts a dialect atom to its corresponding dialect string.
  """
  @spec to_dialect(dialects()) :: String.t()
  def to_dialect(:bigquery), do: "bigquery"
end
