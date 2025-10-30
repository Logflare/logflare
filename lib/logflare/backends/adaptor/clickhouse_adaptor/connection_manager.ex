defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.ConnectionManager do
  @moduledoc """
  Manages a ClickHouse connection pool lifecycle.

  Query/read connection pools are keyed by `Backend`.
  Ingest/write connection pools are keyed by `{Source, Backend}`.
  """

  use GenServer
  use TypedStruct

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source

  @type source_backend_tuple :: {Source.t(), Backend.t()}
  @type connection_type :: :ingest | :query

  @inactivity_timeout :timer.minutes(5)
  @resolve_interval :timer.seconds(30)

  typedstruct do
    field :pool_type, connection_type(), enforce: true
    field :source_id, pos_integer() | nil, default: nil
    field :backend_id, pos_integer(), enforce: true
    field :pool_pid, pid() | nil, default: nil
    field :last_activity, integer() | nil, default: nil
    field :resolve_timer_ref, reference() | nil, default: nil
  end

  @doc """
  Starts a ClickHouse connection manager process.

  These processes have unique behavior based on the arguments provided.

  If providing a `Source` and `Backend`, it is assumed this is an ingest/write connection pool.
  If only providing a `Backend`, it is assumed this is a query/read connection pool.
  """
  @spec start_link(
          {Source.t(), Backend.t()}
          | Backend.t()
        ) :: GenServer.on_start()
  def start_link({%Source{} = source, %Backend{} = backend}) do
    GenServer.start_link(__MODULE__, {source, backend},
      name: connection_manager_via({source, backend})
    )
  end

  def start_link(%Backend{} = backend) do
    GenServer.start_link(__MODULE__, backend, name: connection_manager_via(backend))
  end

  @doc false
  @spec child_spec(
          {Source.t(), Backend.t()}
          | Backend.t()
        ) :: Supervisor.child_spec()
  def child_spec({%Source{} = source, %Backend{} = backend}) do
    %{
      id: {__MODULE__, {source.id, backend.id}},
      start: {__MODULE__, :start_link, [{source, backend}]}
    }
  end

  def child_spec(%Backend{} = backend) do
    %{
      id: {__MODULE__, {backend.id}},
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc """
  Generates a unique ClickHouse connection pool via tuple based on a `{Source, Backend}` or `Backend`.
  """
  @spec connection_pool_via({Source.t(), Backend.t()} | Backend.t()) :: tuple()
  def connection_pool_via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, CHWritePool, backend)
  end

  def connection_pool_via(%Backend{} = backend) do
    Backends.via_backend(backend, CHReadPool)
  end

  @doc """
  Notifies the connection manager of activity.

  This resets the inactivity timer for connections.
  """
  @spec notify_activity({Source.t(), Backend.t()} | Backend.t()) :: :ok
  def notify_activity({%Source{}, %Backend{}} = args) do
    args
    |> connection_manager_via()
    |> GenServer.cast(:update_activity)
  end

  def notify_activity(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.cast(:update_activity)
  end

  @doc """
  Ensures a connection pool has been started.

  This will start the connection pool if it's not already running and notifies the manager of activity.
  """
  @spec ensure_pool_started({Source.t(), Backend.t()} | Backend.t()) :: :ok | {:error, term()}
  def ensure_pool_started({%Source{}, %Backend{}} = args) do
    args
    |> connection_manager_via()
    |> GenServer.call(:ensure_pool)
  end

  def ensure_pool_started(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:ensure_pool)
  end

  @doc """
  Checks if a connection pool is currently active or not.
  """
  @spec pool_active?({Source.t(), Backend.t()} | Backend.t()) :: boolean()
  def pool_active?({%Source{}, %Backend{}} = args) do
    conn_mgr = connection_manager_via(args)

    try do
      GenServer.call(conn_mgr, :pool_active)
    catch
      :exit, _ -> false
    end
  end

  def pool_active?(%Backend{} = backend) do
    conn_mgr = connection_manager_via(backend)

    try do
      GenServer.call(conn_mgr, :pool_active)
    catch
      :exit, _ -> false
    end
  end

  @impl true
  def init(args) do
    resolve_timer_ref = Process.send_after(self(), :resolve_connections, @resolve_interval)

    initial_state =
      case args do
        {%Source{id: source_id}, %Backend{id: backend_id}} ->
          %__MODULE__{
            pool_type: :ingest,
            source_id: source_id,
            backend_id: backend_id,
            resolve_timer_ref: resolve_timer_ref
          }

        %Backend{id: backend_id} ->
          %__MODULE__{
            pool_type: :query,
            source_id: nil,
            backend_id: backend_id,
            resolve_timer_ref: resolve_timer_ref
          }
      end

    {:ok, initial_state}
  end

  @impl true
  def handle_call(:ensure_pool, _from, %__MODULE__{pool_pid: pool_pid} = state)
      when is_pid(pool_pid) do
    Logger.debug(
      "[ClickHouse.ConnectionManager] ensure_pool called with existing pool_pid=#{inspect(pool_pid)} alive=#{Process.alive?(pool_pid)}, backend_id=#{inspect(state.backend_id)}, source_id=#{inspect(state.source_id)}"
    )

    {result, new_state} =
      if Process.alive?(pool_pid) do
        {:ok, record_activity(state)}
      else
        start_pool(state)
      end

    {:reply, result, new_state}
  end

  def handle_call(:ensure_pool, _from, %__MODULE__{} = state) do
    Logger.info(
      "[ClickHouse.ConnectionManager] ensure_pool called without pool_pid, starting new pool. backend_id=#{inspect(state.backend_id)}, source_id=#{inspect(state.source_id)}"
    )

    {result, new_state} = start_pool(state)
    {:reply, result, new_state}
  end

  @impl true
  def handle_call(:pool_active, _from, %__MODULE__{pool_pid: pool_pid} = state)
      when is_pid(pool_pid) do
    {:reply, Process.alive?(pool_pid), state}
  end

  def handle_call(:pool_active, _from, %__MODULE__{} = state), do: {:reply, false, state}

  @impl true
  def handle_cast(:update_activity, %__MODULE__{} = state), do: {:noreply, record_activity(state)}

  @impl true
  def handle_info(:resolve_connections, %__MODULE__{} = state) do
    if state.resolve_timer_ref do
      Process.cancel_timer(state.resolve_timer_ref)
    end

    new_timer_ref = Process.send_after(self(), :resolve_connections, @resolve_interval)

    new_state =
      %__MODULE__{state | resolve_timer_ref: new_timer_ref}
      |> resolve_connection_state()

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %__MODULE__{pool_pid: pid} = state)
      when is_pid(pid) do
    Logger.warning("Clickhouse connection pool died (#{state.pool_type})",
      source_id: state.source_id,
      backend_id: state.backend_id
    )

    {:noreply, %{state | pool_pid: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @spec record_activity(__MODULE__.t()) :: __MODULE__.t()
  defp record_activity(%__MODULE__{} = state) do
    %{state | last_activity: System.system_time(:millisecond)}
  end

  @spec start_pool(__MODULE__.t()) :: {:ok, __MODULE__.t()} | {:error, any()}
  defp start_pool(%__MODULE__{} = state) do
    with {:ok, ch_opts} <- build_ch_opts(state),
         {:ok, pid} <- Ch.start_link(ch_opts) do
      Process.monitor(pid)

      Logger.info("Started Clickhouse connection pool (#{state.pool_type})",
        backend_id: state.backend_id
      )

      new_state =
        %__MODULE__{state | pool_pid: pid}
        |> record_activity()

      {:ok, new_state}
    else
      {:error, reason} ->
        Logger.error(
          "Failed to start Clickhouse connection pool (#{state.pool_type})",
          backend_id: state.backend_id,
          reason: reason
        )

        {{:error, reason}, state}
    end
  end

  @spec resolve_connection_state(%__MODULE__{}) :: %__MODULE__{}
  defp resolve_connection_state(
         %__MODULE__{pool_pid: pool_pid, last_activity: last_activity} = state
       ) do
    now = System.system_time(:millisecond)

    cond do
      # No pool, nothing to do
      is_nil(pool_pid) ->
        state

      # Pool exists but not alive, clean up
      not Process.alive?(pool_pid) ->
        %__MODULE__{state | pool_pid: nil}

      # No activity recorded yet, set activity to now
      is_nil(last_activity) ->
        record_activity(state)

      # Activity within timeout, keep connection
      now - last_activity < @inactivity_timeout ->
        state

      # Inactive for too long, stop connection pool
      true ->
        stop_pool(state)
    end
  end

  @spec stop_pool(%__MODULE__{}) :: %__MODULE__{}
  defp stop_pool(%__MODULE__{pool_pid: pool_pid} = state) when is_pid(pool_pid) do
    if Process.alive?(pool_pid) do
      GenServer.stop(pool_pid)

      Logger.info("Stopped Clickhouse connection pool due to inactivity",
        source_id: state.source_id,
        backend_id: state.backend_id
      )
    end

    %__MODULE__{state | pool_pid: nil}
  end

  @spec build_ch_opts(__MODULE__.t()) :: {:ok, Keyword.t()} | {:error, term()}
  defp build_ch_opts(%__MODULE__{
         backend_id: nil,
         source_id: source_id,
         pool_type: pool_type
       }) do
    # Single-tenant mode: use config from environment
    config = SingleTenant.clickhouse_backend_adapter_opts() || %{}

    Logger.info(
      "[ClickHouse.ConnectionManager] build_ch_opts for single tenant mode, source_id=#{inspect(source_id)}, pool_type=#{pool_type}, config=#{inspect(config)}"
    )

    default_pool_size =
      Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:pool_size]

    pool_size = Keyword.get(config, :pool_size, default_pool_size)

    # Adjust pool size for query pools
    pool_size =
      if pool_type == :query do
        pool_size
        |> div(2)
        |> max(default_pool_size)
      else
        pool_size
      end

    url = Keyword.get(config, :url)

    with {:ok, {scheme, hostname}} <- extract_scheme_and_hostname(url) do
      # Create a virtual backend for pool naming
      virtual_backend = %Backend{
        id: nil,
        type: :clickhouse,
        config: Map.new(config)
      }

      pool_via =
        if is_nil(source_id) do
          virtual_backend
        else
          source = Logflare.Sources.Cache.get_by_id(source_id)
          {source, virtual_backend}
        end
        |> connection_pool_via()

      timeout = if pool_type == :ingest, do: :timer.seconds(15), else: :timer.minutes(1)

      ch_opts = [
        name: pool_via,
        scheme: scheme,
        hostname: hostname,
        port: Keyword.get(config, :port, extract_port_from_url(url)),
        database: Keyword.get(config, :database),
        username: Keyword.get(config, :username),
        password: Keyword.get(config, :password),
        pool_size: pool_size,
        settings: [],
        timeout: timeout
      ]

      {:ok, ch_opts}
    end
  end

  defp build_ch_opts(%__MODULE__{
         backend_id: backend_id,
         source_id: source_id,
         pool_type: pool_type
       }) do
    # Multi-tenant mode: fetch fresh backend from cache
    backend = Backends.Cache.get_backend(backend_id)

    if is_nil(backend) do
      {:error, :backend_not_found}
    else
      config = backend.config

      default_pool_size =
        Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:pool_size]

      pool_size = Map.get(config, :pool_size, default_pool_size)

      # Adjust pool size for query pools
      pool_size =
        if pool_type == :query do
          pool_size
          |> div(2)
          |> max(default_pool_size)
        else
          pool_size
        end

      url = Map.get(config, :url)

      with {:ok, {scheme, hostname}} <- extract_scheme_and_hostname(url) do
        pool_via =
          if is_nil(source_id) do
            backend
          else
            source = Logflare.Sources.Cache.get_by_id(source_id)
            {source, backend}
          end
          |> connection_pool_via()

        timeout = if pool_type == :ingest, do: :timer.seconds(15), else: :timer.minutes(1)

        ch_opts = [
          name: pool_via,
          scheme: scheme,
          hostname: hostname,
          port: get_port_config(backend, config),
          database: config.database,
          username: config.username,
          password: config.password,
          pool_size: pool_size,
          settings: [],
          timeout: timeout
        ]

        {:ok, ch_opts}
      end
    end
  end

  defp extract_port_from_url(url) when is_binary(url) do
    case URI.parse(url) do
      %URI{port: port} when is_integer(port) -> port
      _ -> 8123
    end
  end

  defp extract_port_from_url(_), do: 8123

  @spec extract_scheme_and_hostname(String.t()) ::
          {:ok, {String.t(), String.t()}} | {:error, String.t()}
  defp extract_scheme_and_hostname(url) when is_binary(url) and byte_size(url) > 0 do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, host: hostname}} when scheme in ~w(http https) ->
        {:ok, {scheme, hostname}}

      {:ok, %URI{}} ->
        {:error, "Unable to extract scheme and hostname from URL."}

      {:error, _err_msg} = error ->
        error
    end
  end

  defp extract_scheme_and_hostname(_url), do: {:error, "Unexpected URL value provided."}

  @spec get_port_config(Backend.t(), map()) :: non_neg_integer()
  defp get_port_config(_backend, %{port: port}) when is_pos_integer(port), do: port

  defp get_port_config(_backend, %{port: port}) when is_non_empty_binary(port),
    do: String.to_integer(port)

  @spec connection_manager_via({Source.t(), Backend.t()} | Backend.t()) :: tuple()
  defp connection_manager_via({%Source{} = source, %Backend{} = backend}) do
    Backends.via_source(source, __MODULE__, backend)
  end

  defp connection_manager_via(%Backend{} = backend) do
    Backends.via_backend(backend, __MODULE__)
  end
end
