defmodule Logflare.Sql.DialectTransformer.Clickhouse do
  @moduledoc """
  ClickHouse-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  alias Logflare.Backends.Adaptor.ClickhouseAdaptor

  @impl true
  def quote_style, do: nil

  @impl true
  def dialect, do: "clickhouse"

  @impl true
  def transform_source_name(source_name, %{sources: sources}) do
    source = Enum.find(sources, fn s -> s.name == source_name end)
    ClickhouseAdaptor.clickhouse_ingest_table_name(source)
  end
end
