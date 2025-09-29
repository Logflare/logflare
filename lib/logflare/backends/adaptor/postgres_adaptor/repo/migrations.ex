defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations do
  @moduledoc false

  alias Logflare.Sources.Source
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo, as: Repo
  alias Logflare.Backends.Adaptor.PostgresAdaptor, as: Adaptor

  @migrations [
    :create_table
  ]

  def migrate(source, version \\ 0) do
    table_name = Adaptor.table_name(source)

    Repo.checkout(
      fn ->
        @migrations
        |> Enum.drop(version)
        |> Enum.each(fn name ->
          apply(__MODULE__, name, [table_name])
        end)
      end,
      timeout: 30_000
    )
  end

  def create_table(table_name) do
    execute!("""
    CREATE TABLE IF NOT EXISTS #{table_name} (
      id TEXT PRIMARY KEY,
      body JSONB,
      event_message TEXT,
      timestamp TIMESTAMP
    )
    """)

    execute!("""
    CREATE INDEX IF NOT EXISTS #{table_name}_timestamp_brin_idx ON #{table_name} USING brin (timestamp)
    """)
  end

  @spec down(Source.t()) :: Ecto.Adapters.SQL.query_result()
  def down(source) do
    table_name = Adaptor.table_name(source)

    execute!("TRUNCATE #{table_name}")
  end

  defp execute!(query) do
    Ecto.Adapters.SQL.query!(Repo.get_dynamic_repo(), query)
  end
end
