defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.SchemaCache do
  @moduledoc """
  ETS-backed cache for ClickHouse native protocol column schemas.

  Caches column types returned by the server during INSERT handshakes so that
  subsequent inserts can pre-encode data blocks before sending the query.
  One cache entry per backend + table combination, shared across all pool
  connections.

  Started globally from `Logflare.Backends.Supervisor`.
  """

  use GenServer

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection

  @table :native_ingester_schema_cache

  @spec start_link(term()) :: GenServer.on_start()
  def start_link(_arg) do
    GenServer.start_link(__MODULE__, :ok, name: __MODULE__)
  end

  @spec get(pos_integer(), String.t()) :: Connection.column_info() | nil
  def get(backend_id, cache_key)
      when is_integer(backend_id) and is_non_empty_binary(cache_key) do
    case :ets.lookup(@table, {backend_id, cache_key}) do
      [{_, schema}] -> schema
      [] -> nil
    end
  end

  @spec put(pos_integer(), String.t(), Connection.column_info()) :: true
  def put(backend_id, cache_key, schema)
      when is_integer(backend_id) and is_non_empty_binary(cache_key) and is_list(schema) do
    :ets.insert(@table, {{backend_id, cache_key}, schema})
  end

  @impl GenServer
  def init(:ok) do
    :ets.new(@table, [:named_table, :set, :public, read_concurrency: true])
    {:ok, %{}}
  end
end
