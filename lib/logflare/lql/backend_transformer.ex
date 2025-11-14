defmodule Logflare.Lql.BackendTransformer do
  @moduledoc """
  Behaviour for LQL-to-backend query transformations.

  This module defines the interface that all LQL backend transformer implementations must follow.

  Each backend transformer (BigQuery, ClickHouse, PostgreSQL, etc.) must implement this
  behaviour to provide backend-specific query translation from
  LQL `ChartRule`, `FilterRule`, and `SelectRule` structs.
  """

  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Lql.Rules.SelectRule
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

  @type dialects :: :bigquery | :clickhouse | :postgres

  @doc """
  Transforms a single LQL FilterRule into a backend-specific query fragment.

  This is the core function that converts LQL filtering logic into the
  appropriate query structure for the backend (Ecto dynamic query, raw SQL, etc.).
  """
  @callback transform_filter_rule(FilterRule.t(), transformation_data()) :: term()

  @doc """
  Transforms a `ChartRule` into a backend-specific chart query with time-series aggregation.
  """
  @callback transform_chart_rule(
              query :: Ecto.Query.t(),
              aggregate :: :count | :avg | :sum | :max | :p50 | :p95 | :p99,
              field_path :: String.t(),
              period :: :second | :minute | :hour | :day,
              timestamp_field :: String.t()
            ) :: Ecto.Query.t()

  @doc """
  Applies multiple LQL `FilterRules` to a query, returning the modified query.

  This is the main entry point for applying LQL filters to a backend query.
  """
  @callback apply_filter_rules_to_query(query :: term(), [FilterRule.t()], keyword()) :: term()

  @doc """
  Transforms a single LQL `SelectRule` into a backend-specific field selection.

  Converts LQL field selection rules into backend-specific query structures.
  """
  @callback transform_select_rule(SelectRule.t(), transformation_data()) :: term()

  @doc """
  Applies a list of `SelectRule` structs to modify query field selection.

  This function processes field selection rules and applies them to the query,
  handling both wildcard selection and specific field projections.
  """
  @callback apply_select_rules_to_query(Ecto.Query.t(), [SelectRule.t()], Keyword.t()) ::
              Ecto.Query.t()

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
  def for_dialect("clickhouse"), do: __MODULE__.ClickHouse
  def for_dialect("postgres"), do: __MODULE__.Postgres
  def for_dialect(value) when is_atom(value), do: value |> to_dialect() |> for_dialect()

  @doc """
  Converts a dialect atom to its corresponding dialect string.
  """
  @spec to_dialect(dialects()) :: String.t()
  def to_dialect(:bigquery), do: "bigquery"
  def to_dialect(:clickhouse), do: "clickhouse"
  def to_dialect(:postgres), do: "postgres"
end
