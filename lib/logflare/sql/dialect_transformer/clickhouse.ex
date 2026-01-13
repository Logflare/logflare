defmodule Logflare.Sql.DialectTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.User

  @impl true
  def quote_style, do: nil

  @impl true
  def dialect, do: "clickhouse"

  @impl true
  def transform_source_name(source_name, %{sources: sources}) do
    source = Enum.find(sources, fn s -> s.name == source_name end)

    case find_clickhouse_backend(source) do
      nil ->
        raise "No ClickHouse backend found for source '#{source_name}'. " <>
                "Sources must be associated with a ClickHouse backend for SQL transformation."

      backend ->
        ClickHouseAdaptor.clickhouse_ingest_table_name(backend)
    end
  end

  @doc """
  Builds transformation data for ClickHouse from a user and base data.

  Since ClickHouse does not require project/dataset metadata, we can just pass through the base data.
  """
  @spec build_transformation_data(User.t(), map()) :: map()
  def build_transformation_data(%User{}, base_data), do: base_data

  @spec find_clickhouse_backend(Source.t()) :: Backend.t() | nil
  defp find_clickhouse_backend(%Source{} = source) do
    source
    |> Sources.preload_backends()
    |> Map.get(:backends, [])
    |> Enum.filter(fn backend -> backend.type == :clickhouse end)
    |> case do
      [] ->
        nil

      [backend] ->
        backend

      [_ | _] ->
        raise "Multiple ClickHouse backends found for source '#{source.name}'. " <>
                "Sources with multiple ClickHouse backends are not supported for SQL transformation."
    end
  end
end
