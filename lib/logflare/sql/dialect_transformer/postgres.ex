defmodule Logflare.Sql.DialectTransformer.Postgres do
  @moduledoc """
  PostgreSQL-specific SQL transformations.
  """

  @behaviour Logflare.Sql.DialectTransformer

  alias Logflare.Backends.Adaptor.PostgresAdaptor

  @impl true
  def quote_style, do: "\""

  @impl true
  def dialect, do: "postgres"

  @impl true
  def transform_source_name(source_name, %{sources: sources}) do
    source = Enum.find(sources, fn s -> s.name == source_name end)
    PostgresAdaptor.table_name(source)
  end
end
