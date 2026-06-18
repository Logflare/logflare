defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager do
  @moduledoc """
  Manages a ClickHouse connection pool lifecycle for query/read operations.

  Connection pools are keyed by `Backend`.

  Active pools have their connections periodically recycled, gracefully
  disconnected and immediately re-established so that upstream
  "least-connections" load balancers can redistribute them across replicas as
  the ClickHouse service scales. See `recycle_pool/1`.

  Pools capture backend configuration when they start. `refresh_pool/1` stops an
  active pool so that the next query restarts it with freshly loaded configuration.
  """

  use GenServer
  use TypedStruct

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Backend

  @inactivity_timeout :timer.minutes(5)
  @resolve_interval :timer.seconds(30)
  @ch_query_conn_timeout :timer.minutes(1)
  @recycle_interval :timer.minutes(10)
  @recycle_spread :timer.seconds(60)

  typedstruct do
    field :backend_id, pos_integer(), enforce: true
    field :pool_pid, pid() | nil, default: nil
    field :last_activity, integer() | nil, default: nil
    field :resolve_timer_ref, reference() | nil, default: nil
    field :next_recycle_at, integer() | nil, default: nil
  end

  @doc """
  Starts a ClickHouse connection manager process for query operations.
  """
  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{id: backend_id} = backend) do
    GenServer.start_link(__MODULE__, backend_id, name: connection_manager_via(backend))
  end

  @doc false
  @spec child_spec(Backend.t()) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend) do
    %{
      id: {__MODULE__, backend.id},
      start: {__MODULE__, :start_link, [backend]}
    }
  end

  @doc """
  Generates a unique ClickHouse connection pool via tuple based on a `Backend`.
  """
  @spec connection_pool_via(Backend.t()) :: tuple()
  def connection_pool_via(%Backend{} = backend) do
    Backends.via_backend(backend, CHReadPool)
  end

  @doc """
  Notifies the connection manager of activity.

  This resets the inactivity timer for connections.
  """
  @spec notify_activity(Backend.t()) :: :ok
  def notify_activity(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.cast(:update_activity)
  end

  @doc """
  Ensures a connection pool has been started.

  This will start the connection pool if it's not already running and notifies the manager of activity.
  """
  @spec ensure_pool_started(Backend.t()) :: :ok | {:error, term()}
  def ensure_pool_started(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:ensure_pool)
  end

  @doc """
  Checks if a connection pool is currently active or not.
  """
  @spec pool_active?(Backend.t()) :: boolean()
  def pool_active?(%Backend{} = backend) do
    conn_mgr = connection_manager_via(backend)

    try do
      GenServer.call(conn_mgr, :pool_active)
    catch
      :exit, _ -> false
    end
  end

  @doc """
  Immediately recycles the active connection pool for a backend.

  Pooled connections are disconnected gracefully - busy connections as their
  current queries finish, idle connections on the next idle sweep - and are
  immediately re-established by the pool. This gives a "least-connections" load
  balancer the chance to redistribute connections across replicas.
  Useful as a manual tool after scaling up a ClickHouse service.
  """
  @spec recycle_pool(Backend.t() | pid()) :: :ok | {:error, :no_pool}
  def recycle_pool(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:recycle_pool)
  end

  def recycle_pool(manager_pid) when is_pid(manager_pid) do
    GenServer.call(manager_pid, :recycle_pool)
  end

  @doc """
  Stops the active connection pool for a backend so the next query restarts it
  with freshly loaded backend configuration.

  Unlike `recycle_pool/1`, this is a hard restart: in-flight queries on the pool
  will fail. Intended for backend configuration changes (e.g. rotated credentials
  or a changed URL), since pooled connections capture their connection options
  when the pool starts.
  """
  @spec refresh_pool(Backend.t() | pid()) :: :ok
  def refresh_pool(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:refresh_pool)
  end

  def refresh_pool(manager_pid) when is_pid(manager_pid) do
    GenServer.call(manager_pid, :refresh_pool)
  end

  @doc """
  Gets the last activity timestamp for the connection manager.
  """
  @spec get_last_activity(Backend.t()) :: integer() | nil
  def get_last_activity(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:get_last_activity)
  end

  @doc """
  Sets the last activity timestamp.
  """
  @spec set_last_activity(Backend.t(), integer() | nil) :: :ok
  def set_last_activity(%Backend{} = backend, timestamp) do
    backend
    |> connection_manager_via()
    |> GenServer.cast({:set_last_activity, timestamp})
  end

  @doc """
  Returns the PID of the active connection pool, or `nil` if no pool is running.
  """
  @spec get_pool_pid(Backend.t()) :: pid() | nil
  def get_pool_pid(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:get_pool_pid)
  end

  @doc """
  Returns the timestamp at which the active pool's connections are next scheduled
  to be recycled, or `nil` if no pool is running.
  """
  @spec get_next_recycle_at(Backend.t()) :: integer() | nil
  def get_next_recycle_at(%Backend{} = backend) do
    backend
    |> connection_manager_via()
    |> GenServer.call(:get_next_recycle_at)
  end

  @impl true
  def init(backend_id) do
    resolve_timer_ref = resolve_timer_send_after()

    initial_state = %__MODULE__{
      backend_id: backend_id,
      resolve_timer_ref: resolve_timer_ref
    }

    {:ok, initial_state}
  end

  @impl true
  def handle_call(:ensure_pool, _from, %__MODULE__{pool_pid: pool_pid} = state)
      when is_pid(pool_pid) do
    {result, new_state} =
      if Process.alive?(pool_pid) do
        {:ok, record_activity(state)}
      else
        start_pool(state)
      end

    {:reply, result, new_state}
  end

  def handle_call(:ensure_pool, _from, %__MODULE__{} = state) do
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
  def handle_call(:recycle_pool, _from, %__MODULE__{pool_pid: pool_pid} = state)
      when is_pid(pool_pid) do
    if Process.alive?(pool_pid) do
      new_state = recycle_pool_connections(state, System.system_time(:millisecond))
      {:reply, :ok, new_state}
    else
      {:reply, {:error, :no_pool}, %__MODULE__{state | pool_pid: nil, next_recycle_at: nil}}
    end
  end

  def handle_call(:recycle_pool, _from, %__MODULE__{} = state) do
    {:reply, {:error, :no_pool}, state}
  end

  @impl true
  def handle_call(:refresh_pool, _from, %__MODULE__{pool_pid: pool_pid} = state)
      when is_pid(pool_pid) do
    if Process.alive?(pool_pid) do
      GenServer.stop(pool_pid)
    end

    {:reply, :ok, %__MODULE__{state | pool_pid: nil, next_recycle_at: nil}}
  end

  def handle_call(:refresh_pool, _from, %__MODULE__{} = state) do
    {:reply, :ok, %__MODULE__{state | pool_pid: nil, next_recycle_at: nil}}
  end

  @impl true
  def handle_call(:get_last_activity, _from, %__MODULE__{last_activity: last_activity} = state) do
    {:reply, last_activity, state}
  end

  @impl true
  def handle_call(:get_pool_pid, _from, %__MODULE__{pool_pid: pool_pid} = state) do
    {:reply, pool_pid, state}
  end

  @impl true
  def handle_call(
        :get_next_recycle_at,
        _from,
        %__MODULE__{next_recycle_at: next_recycle_at} = state
      ) do
    {:reply, next_recycle_at, state}
  end

  @impl true
  def handle_cast(:update_activity, %__MODULE__{} = state), do: {:noreply, record_activity(state)}

  @impl true
  def handle_cast({:set_last_activity, timestamp}, %__MODULE__{} = state) do
    {:noreply, %{state | last_activity: timestamp}}
  end

  @impl true
  def handle_info(:resolve_connections, %__MODULE__{} = state) do
    if state.resolve_timer_ref do
      Process.cancel_timer(state.resolve_timer_ref)
    end

    new_timer_ref = resolve_timer_send_after()

    new_state =
      %__MODULE__{state | resolve_timer_ref: new_timer_ref}
      |> resolve_connection_state()

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %__MODULE__{pool_pid: pid} = state)
      when is_pid(pid) do
    Logger.warning("ClickHouse connection pool died",
      backend_id: state.backend_id,
      host: connection_host(state.backend_id)
    )

    {:noreply, %{state | pool_pid: nil, next_recycle_at: nil}}
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

      Logger.info("Started ClickHouse connection pool",
        backend_id: state.backend_id
      )

      new_state =
        %__MODULE__{state | pool_pid: pid}
        |> record_activity()
        |> schedule_next_recycle(System.system_time(:millisecond))

      {:ok, new_state}
    else
      {:error, reason} ->
        Logger.error(
          "Failed to start ClickHouse connection pool",
          backend_id: state.backend_id,
          host: connection_host(state.backend_id),
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
        %__MODULE__{state | pool_pid: nil, next_recycle_at: nil}

      # No activity recorded yet, set activity to now
      is_nil(last_activity) ->
        record_activity(state)

      # Activity within timeout, keep pool and recycle connections when due
      now - last_activity < @inactivity_timeout ->
        maybe_recycle_pool(state, now)

      # Inactive for too long, stop connection pool
      true ->
        stop_pool(state)
    end
  end

  @spec stop_pool(%__MODULE__{}) :: %__MODULE__{}
  defp stop_pool(%__MODULE__{pool_pid: pool_pid} = state) when is_pid(pool_pid) do
    if Process.alive?(pool_pid) do
      GenServer.stop(pool_pid)

      Logger.info("Stopped ClickHouse connection pool due to inactivity",
        backend_id: state.backend_id
      )
    end

    %__MODULE__{state | pool_pid: nil, next_recycle_at: nil}
  end

  @spec maybe_recycle_pool(__MODULE__.t(), integer()) :: __MODULE__.t()
  defp maybe_recycle_pool(%__MODULE__{next_recycle_at: nil} = state, now),
    do: schedule_next_recycle(state, now)

  defp maybe_recycle_pool(%__MODULE__{next_recycle_at: next_recycle_at} = state, now)
       when now >= next_recycle_at do
    Logger.info("Recycling ClickHouse read pool connections",
      backend_id: state.backend_id
    )

    recycle_pool_connections(state, now)
  end

  defp maybe_recycle_pool(%__MODULE__{} = state, _now), do: state

  @spec recycle_pool_connections(__MODULE__.t(), integer()) :: __MODULE__.t()
  defp recycle_pool_connections(%__MODULE__{pool_pid: pool_pid} = state, now)
       when is_pid(pool_pid) do
    DBConnection.disconnect_all(pool_pid, recycle_spread())

    schedule_next_recycle(state, now)
  end

  @spec schedule_next_recycle(__MODULE__.t(), integer()) :: __MODULE__.t()
  defp schedule_next_recycle(%__MODULE__{} = state, now) do
    interval = recycle_interval()
    jitter = div(interval, 5)
    offset = :rand.uniform(2 * jitter + 1) - jitter - 1

    %__MODULE__{state | next_recycle_at: now + interval + offset}
  end

  @spec recycle_interval() :: pos_integer()
  defp recycle_interval do
    Application.get_env(:logflare, __MODULE__)[:recycle_interval] || @recycle_interval
  end

  @spec recycle_spread() :: pos_integer()
  defp recycle_spread do
    Application.get_env(:logflare, __MODULE__)[:recycle_spread] || @recycle_spread
  end

  @spec build_ch_opts(__MODULE__.t()) :: {:ok, Keyword.t()} | {:error, term()}
  defp build_ch_opts(%__MODULE__{backend_id: backend_id}) do
    # Fetch fresh backend from cache
    backend = Backends.Cache.get_backend(backend_id)

    if is_nil(backend) do
      {:error, :backend_not_found}
    else
      config = backend.config

      default_pool_size =
        Application.fetch_env!(:logflare, :clickhouse_backend_adaptor)[:pool_size]

      pool_size =
        config
        |> Map.get(:pool_size, default_pool_size)
        |> div(2)
        |> max(default_pool_size)

      url = read_url(config)

      with {:ok, {scheme, hostname, url_port}} <- extract_url_components(url) do
        pool_via = connection_pool_via(backend)
        port = get_read_port(config, url_port)

        ch_opts = [
          name: pool_via,
          scheme: scheme,
          hostname: hostname,
          port: port,
          database: config.database,
          username: config.username,
          password: config.password,
          pool_size: pool_size,
          settings: [],
          timeout: @ch_query_conn_timeout
        ]

        {:ok, ch_opts}
      end
    end
  end

  @spec read_url(map()) :: String.t() | nil
  defp read_url(config) do
    read_only_url = Map.get(config, :read_only_url)
    if is_non_empty_binary(read_only_url), do: read_only_url, else: Map.get(config, :url)
  end

  @spec connection_host(pos_integer()) :: String.t() | nil
  defp connection_host(backend_id) do
    with %Backend{config: config} <- Backends.Cache.get_backend(backend_id),
         {:ok, {_scheme, hostname, _port}} <- extract_url_components(read_url(config)) do
      hostname
    else
      _ -> nil
    end
  end

  @spec extract_url_components(String.t()) ::
          {:ok, {String.t(), String.t(), non_neg_integer() | nil}} | {:error, String.t()}
  defp extract_url_components(url) when is_non_empty_binary(url) do
    case URI.new(url) do
      {:ok, %URI{scheme: scheme, host: hostname, port: port}} when scheme in ~w(http https) ->
        {:ok, {scheme, hostname, port}}

      {:ok, %URI{}} ->
        {:error, "Unable to extract scheme and hostname from URL."}

      {:error, _err_msg} = error ->
        error
    end
  end

  defp extract_url_components(_url), do: {:error, "Unexpected URL value provided."}

  @spec get_read_port(map(), non_neg_integer() | nil) :: non_neg_integer()
  defp get_read_port(config, url_port), do: url_port || get_config_port(config)

  @spec get_config_port(map()) :: non_neg_integer()
  defp get_config_port(%{port: port}) when is_pos_integer(port), do: port
  defp get_config_port(%{port: port}) when is_non_empty_binary(port), do: String.to_integer(port)

  @spec connection_manager_via(Backend.t()) :: tuple()
  defp connection_manager_via(%Backend{} = backend) do
    Backends.via_backend(backend, __MODULE__)
  end

  defp resolve_timer_send_after do
    resolve_interval =
      Application.get_env(:logflare, __MODULE__)[:resolve_interval] || @resolve_interval

    Process.send_after(self(), :resolve_connections, resolve_interval)
  end
end
