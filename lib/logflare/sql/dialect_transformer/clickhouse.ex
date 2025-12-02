defmodule Logflare.Sql.DialectTransformer.ClickHouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.User

  @impl true
  def quote_style, do: nil

  @impl true
  def dialect, do: "clickhouse"

  @impl true
  def transform_source_name(source_name, %{sources: sources}) do
    source = Enum.find(sources, fn s -> s.name == source_name end)
    ClickHouseAdaptor.clickhouse_ingest_table_name(source)
  end

  @doc """
  Builds transformation data for ClickHouse from a user and base data.

  Since ClickHouse does not require project/dataset metadata, we can just pass through the base data.
  """
  @spec build_transformation_data(User.t(), map()) :: map()
  def build_transformation_data(%User{}, base_data), do: base_data
end
