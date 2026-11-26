defmodule Logflare.Backends.Adaptor.PostgresAdaptor.Repo.Migrations do
  @moduledoc false

  alias Logflare.Sources.Source
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo, as: Repo
  alias Logflare.Backends.Adaptor.PostgresAdaptor, as: Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.UserMonitoring.QueryTraces
  alias Ecto.Adapters.SQL

  @migrations [
    :create_table
  ]

  def migrate(%Source{} = source, %Backend{} = backend, version \\ 0) do
    table_name = Adaptor.table_name(source)

    Repo.checkout(
      fn ->
        @migrations
        |> Enum.drop(version)
        |> Enum.each(fn name ->
          apply(__MODULE__, name, [table_name, source, backend])
        end)
      end,
      timeout: 30_000
    )
  end

  def create_table(table_name, source, backend) do
    """
    CREATE TABLE IF NOT EXISTS #{table_name} (
      id TEXT PRIMARY KEY,
      body JSONB,
      event_message TEXT,
      timestamp TIMESTAMP
    )
    """
    |> traced_execute!(:create_table, table_name, source, backend)

    """
    CREATE INDEX IF NOT EXISTS #{table_name}_timestamp_brin_idx ON #{table_name} USING brin (timestamp)
    """
    |> traced_execute!(:create_index, table_name, source, backend)
  end

  @spec down(Source.t()) :: Ecto.Adapters.SQL.query_result()
  def down(source) do
    table_name = Adaptor.table_name(source)

    execute!("TRUNCATE #{table_name}")
  end

  defp execute!(query) do
    SQL.query!(Repo.get_dynamic_repo(), query)
  end

  @spec traced_execute!(String.t(), atom(), String.t(), Source.t(), Backend.t()) ::
          Postgrex.Result.t()
  defp traced_execute!(query, step, table_name, source, backend) do
    trace_attrs = %{
      query_type: :provision,
      query: query,
      backend_type: :postgres,
      backend_id: backend.id,
      source_id: source.id,
      source_token: source.token,
      source_name: source.name
    }

    source
    |> trace_user_id()
    |> QueryTraces.measure_once({:postgres, backend.id, table_name, step}, trace_attrs, fn ->
      SQL.query(Repo.get_dynamic_repo(), query)
    end)
    |> case do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @spec trace_user_id(Source.t()) :: pos_integer() | nil
  defp trace_user_id(%Source{system_source: true}), do: nil
  defp trace_user_id(%Source{user_id: user_id}), do: user_id
end
