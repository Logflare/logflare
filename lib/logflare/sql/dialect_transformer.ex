defmodule Logflare.Sql.DialectTransformer do
  @moduledoc """
  Behaviour for dialect-specific SQL transformations.
  """

  import Logflare.Utils.Guards

  alias Logflare.Sources.Source

  @type transformation_data :: %{
          sources: [Source.t()],
          source_mapping: %{String.t() => Source.t()},
          dialect: String.t(),
          ast: any()
        }

  @type dialects :: :bq_sql | :ch_sql | :pg_sql

  @doc """
  Returns the quote style for the dialect.
  """
  @callback quote_style() :: String.t() | nil

  @doc """
  Transforms a source name to the appropriate table reference for this dialect.
  """
  @callback transform_source_name(source_name :: String.t(), data :: transformation_data()) ::
              String.t()

  @doc """
  Returns the dialect string used by the SQL parser.
  """
  @callback dialect() :: String.t()

  @doc """
  Returns the module for a given dialect (atom or string).
  """
  @spec for_dialect(dialects() | String.t()) :: module()
  def for_dialect("bigquery"), do: __MODULE__.BigQuery
  def for_dialect("clickhouse"), do: __MODULE__.ClickHouse
  def for_dialect("postgres"), do: __MODULE__.Postgres
  def for_dialect(value) when is_atom_value(value), do: value |> to_dialect() |> for_dialect()

  @doc """
  Converts a language atom to its corresponding dialect string.
  """
  @spec to_dialect(dialects()) :: String.t()
  def to_dialect(:bq_sql), do: "bigquery"
  def to_dialect(:ch_sql), do: "clickhouse"
  def to_dialect(:pg_sql), do: "postgres"
end
