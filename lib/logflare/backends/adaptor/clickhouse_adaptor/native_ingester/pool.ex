defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool do
  @moduledoc """
  NimblePool-based connection pool for the ClickHouse native TCP protocol.

  Manages a pool of persistent `Connection` structs per backend. Each connection
  is a long-lived TCP socket that has completed the 'Hello' handshake and can be
  reused for multiple INSERT operations.
  """

  @behaviour NimblePool

  require Logger

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Connection
  alias Logflare.Backends.Backend

  @max_pool_size 100
  @checkout_timeout 10_000
  @worker_idle_timeout 30_000

  @default_native_port 9000
  @secure_native_port 9440

  @doc """
  Returns the via tuple for a pool registered under a given backend.
  """
  @spec via(Backend.t()) :: GenServer.name()
  def via(%Backend{} = backend) do
    Logflare.Backends.via_backend(backend, __MODULE__)
  end

  @doc false
  def child_spec(%Backend{} = backend) do
    %{
      id: {__MODULE__, backend.id},
      start: {__MODULE__, :start_link, [backend]},
      type: :worker,
      restart: :permanent
    }
  end

  @doc """
  Starts the connection pool for a backend.

  Extracts connection options (host, port, database, username, password, compression)
  from the backend config and starts a NimblePool with the configured pool size.
  """
  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{} = backend) do
    connect_opts = build_connect_opts(backend)
    pool_size = resolve_pool_size(backend.config)

    NimblePool.start_link(
      worker: {__MODULE__, connect_opts},
      pool_size: pool_size,
      lazy: true,
      worker_idle_timeout: @worker_idle_timeout,
      name: via(backend)
    )
  end

  @doc """
  Returns the min and max pool size constraints for validation.
  """
  @spec pool_size_range() :: {pos_integer(), pos_integer()}
  def pool_size_range, do: {min_pool_size(), @max_pool_size}

  @doc """
  Tests connectivity for a backend without starting the pool.

  Builds connection options from the backend config and performs a
  connect → ping → close cycle. Useful for verifying configuration from IEx.

  ## Example

      iex> backend = Logflare.Backends.get_backend(123)
      iex> Pool.test_connection(backend)
      :ok

  """
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    backend
    |> build_connect_opts()
    |> Connection.test_connection()
  end

  @doc """
  Checks out a connection, passes it to `fun`, and returns it to the pool.

  The callback receives a `Connection` struct and must return a
  `{client_result, checkin_instruction}` tuple where `checkin_instruction` is
  either the (possibly updated) `Connection` struct or `:remove` to discard a
  dead connection.
  """
  @spec checkout(Backend.t(), (Connection.t() -> {term(), Connection.t() | :remove})) :: term()
  def checkout(%Backend{} = backend, fun) when is_function(fun, 1) do
    NimblePool.checkout!(
      via(backend),
      :checkout,
      fn _pool, conn -> fun.(conn) end,
      @checkout_timeout
    )
  end

  @impl NimblePool
  def init_worker(connect_opts) do
    case Connection.connect(connect_opts) do
      {:ok, conn} ->
        {:ok, conn, connect_opts}

      {:error, reason} ->
        Logger.warning("ClickHouse NativeIngester.Pool: failed to connect: #{inspect(reason)}")
        {:error, reason}
    end
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, %Connection{} = conn, pool_state) do
    if Connection.alive?(conn) do
      {:ok, conn, conn, pool_state}
    else
      {:remove, :disconnected, pool_state}
    end
  end

  @impl NimblePool
  def handle_checkin(:remove, _from, _conn, pool_state) do
    {:remove, :connection_error, pool_state}
  end

  def handle_checkin(%Connection{} = conn, _from, _old_conn, pool_state) do
    {:ok, conn, pool_state}
  end

  @impl NimblePool
  def handle_ping(%Connection{} = conn, _pool_state) do
    if Connection.alive?(conn) do
      {:ok, conn}
    else
      {:remove, :idle_stale}
    end
  end

  @impl NimblePool
  def terminate_worker(_reason, conn, pool_state) do
    if is_struct(conn, Connection) do
      Connection.close(conn)
    end

    {:ok, pool_state}
  end

  @spec min_pool_size() :: pos_integer()
  defp min_pool_size, do: System.schedulers_online()

  @spec resolve_pool_size(map()) :: pos_integer()
  defp resolve_pool_size(config) do
    default =
      Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:native_pool_size]

    config
    |> Map.get(:native_pool_size, default)
    |> max(min_pool_size())
    |> min(@max_pool_size)
  end

  @doc false
  @spec build_connect_opts(Backend.t()) :: Connection.connect_opts()
  def build_connect_opts(%Backend{config: config}) do
    url = Map.fetch!(config, :url)
    uri = URI.parse(url)
    host = uri.host

    native_port =
      config |> Map.get(:native_port, @default_native_port) |> to_integer()

    base_opts = [
      host: host,
      port: native_port,
      database: Map.fetch!(config, :database),
      username: Map.fetch!(config, :username),
      password: Map.fetch!(config, :password),
      compression: :lz4
    ]

    if native_port == @secure_native_port do
      Keyword.put(base_opts, :transport, :ssl)
    else
      base_opts
    end
  end

  @spec to_integer(integer() | String.t()) :: integer()
  defp to_integer(val) when is_integer(val), do: val
  defp to_integer(val) when is_binary(val), do: String.to_integer(val)
end
