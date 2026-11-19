defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager do
  @moduledoc """
  Manages a ClickHouse connection pool lifecycle for query/read operations.

  Connection pools are keyed by `Backend`.

  Active pools have their connections periodically recycled, gracefully
  disconnected and immediately re-established so that upstream
  "least-connections" load balancers can redistribute them across replicas as
  the ClickHouse service scales. See `recycle_pool/1`.

  Pools capture backend configuration when they start. `refresh_pool/2` stops an
  active pool whose connection options no longer match the given backend, so that
  the next query restarts it with the current ones.

  Pools authenticate with the credentials resolved by
  `ClickHouseAdaptor.query_credentials/1`, which prefers a
  dedicated query user when one is configured.

  Managers are registered through `Registry`, so they carry no registered name.
  A manager's registry value is `%{pool: pid, pool_size: pos_integer}` while its
  pool runs, and `nil` otherwise. Pools themselves are not registered.
  Each labels itself `{:ch_read_pool_manager, backend_id, read_cluster_tag}` with
  `Process.set_label/1` so that `observer`, `:recon`, crash reports, and the
  `Logflare.Telemetry` top-process metrics can attribute it to a backend and read
  cluster. The third element matches the `read_cluster` telemetry tag.
  """

  use GenServer
  use TypedStruct

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendRegistry

  @inactivity_timeout :timer.minutes(5)
  @resolve_interval :timer.seconds(30)
  @ch_query_conn_timeout :timer.minutes(1)
  @ch_queue_target :timer.seconds(5)
  @recycle_interval :timer.minutes(10)
  @recycle_spread :timer.seconds(60)
  @ch_idle_interval :timer.seconds(3)
  @default_read_pool_size 50
  @default_labeled_read_pool_size 32
  @telemetry_listener __MODULE__.TelemetryListener

  typedstruct do
    field :backend_id, pos_integer(), enforce: true
    field :label, String.t() | nil, default: nil
    field :pool_pid, pid() | nil, default: nil
    field :pool_size, pos_integer() | nil, default: nil
    field :last_activity, integer() | nil, default: nil
    field :resolve_timer_ref, reference() | nil, default: nil
    field :next_recycle_at, integer() | nil, default: nil
    field :pool_opts_hash, binary() | nil, default: nil
  end

  @doc """
  Starts a ClickHouse connection manager process for query operations.

  A `nil` label manages the legacy single read pool; a non-`nil` label manages the
  read pool for that specific read cluster.
  """
  @spec start_link(Backend.t(), String.t() | nil) :: GenServer.on_start()
  def start_link(%Backend{id: backend_id} = backend, label \\ nil) do
    GenServer.start_link(__MODULE__, {backend_id, label},
      name: connection_manager_via(backend, label)
    )
  end

  @doc false
  @spec child_spec(Backend.t(), String.t() | nil) :: Supervisor.child_spec()
  def child_spec(%Backend{} = backend, label \\ nil) do
    %{
      id: {__MODULE__, backend.id, label},
      start: {__MODULE__, :start_link, [backend, label]}
    }
  end

  @doc """
  Registered name of the `DBConnection.TelemetryListener` that read pools report
  connect and disconnect events to.

  The listener is supervised by `QueryConnectionSup` so that it outlives the pools,
  which are stopped and restarted on config refresh, inactivity, and recycle.
  """
  @spec telemetry_listener_name() :: atom()
  def telemetry_listener_name, do: @telemetry_listener

  @doc """
  Notifies the connection manager of activity.

  This resets the inactivity timer for connections.
  """
  @spec notify_activity(Backend.t(), String.t() | nil) :: :ok
  def notify_activity(%Backend{} = backend, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> GenServer.cast(:update_activity)
  end

  @doc """
  Ensures a connection pool has been started.

  This will start the connection pool if it's not already running and notifies the manager of activity.
  """
  @spec ensure_pool_started(Backend.t(), String.t() | nil) :: :ok | {:error, term()}
  def ensure_pool_started(%Backend{} = backend, label \\ nil) do
    with {:ok, _pool} <- fetch_pool(backend, label), do: :ok
  end

  @doc """
  Returns the pool that new queries should run on, starting it if needed, and
  records activity.

  The pool is replaced when it stops or its connection options change, so fetch it
  per query rather than caching the pid.
  """
  @spec fetch_pool(Backend.t(), String.t() | nil) :: {:ok, pid()} | {:error, term()}
  def fetch_pool(%Backend{} = backend, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> GenServer.call(:ensure_pool)
  end

  @doc """
  Checks if a connection pool is currently active or not.
  """
  @spec pool_active?(Backend.t(), String.t() | nil) :: boolean()
  def pool_active?(%Backend{} = backend, label \\ nil) do
    conn_mgr = connection_manager_via(backend, label)

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
  def recycle_pool(%Backend{} = backend), do: recycle_pool(backend, nil)

  def recycle_pool(manager_pid) when is_pid(manager_pid) do
    GenServer.call(manager_pid, :recycle_pool)
  end

  @spec recycle_pool(Backend.t(), String.t() | nil) :: :ok | {:error, :no_pool}
  def recycle_pool(%Backend{} = backend, label) do
    backend
    |> connection_manager_via(label)
    |> GenServer.call(:recycle_pool)
  end

  @doc """
  Stops the backend's active pool for `label` when `backend` yields different
  connection options than it was started with.

  The next query starts a new pool with the new options. Unlike `recycle_pool/1`,
  this is a hard restart: queries running on the stopped pool fail.
  """
  @spec refresh_pool(Backend.t(), String.t() | nil) :: :ok
  def refresh_pool(%Backend{} = backend, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> refresh_manager_pool(backend)
  end

  @doc """
  Same as `refresh_pool/2`, for the connection manager running as `manager`.

  A `nil` backend, such as one deleted since, always stops the pool. A backend
  without a loaded `config` raises `FunctionClauseError`.
  """
  @spec refresh_manager_pool(GenServer.server(), Backend.t() | nil) :: :ok
  def refresh_manager_pool(manager, backend) when is_nil(backend) or is_map(backend.config) do
    GenServer.call(manager, {:refresh_pool, backend})
  end

  @doc """
  Gets the last activity timestamp for the connection manager.
  """
  @spec get_last_activity(Backend.t(), String.t() | nil) :: integer() | nil
  def get_last_activity(%Backend{} = backend, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> GenServer.call(:get_last_activity)
  end

  @doc """
  Sets the last activity timestamp.
  """
  @spec set_last_activity(Backend.t(), integer() | nil, String.t() | nil) :: :ok
  def set_last_activity(%Backend{} = backend, timestamp, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> GenServer.cast({:set_last_activity, timestamp})
  end

  @doc """
  Returns the PID of the active connection pool, or `nil` if no pool is running.
  """
  @spec get_pool_pid(Backend.t(), String.t() | nil) :: pid() | nil
  def get_pool_pid(%Backend{} = backend, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> GenServer.call(:get_pool_pid)
  end

  @doc """
  Returns the timestamp at which the active pool's connections are next scheduled
  to be recycled, or `nil` if no pool is running.
  """
  @spec get_next_recycle_at(Backend.t(), String.t() | nil) :: integer() | nil
  def get_next_recycle_at(%Backend{} = backend, label \\ nil) do
    backend
    |> connection_manager_via(label)
    |> GenServer.call(:get_next_recycle_at)
  end

  @doc """
  Resolves the host for a given `label`.

  Falls back to the `default_read_cluster` entry, then to the primary `url`.
  """
  @spec read_host(Backend.t() | nil, String.t() | nil) :: String.t() | nil
  def read_host(%Backend{config: config}, label) do
    case extract_url_components(read_url(config, label)) do
      {:ok, {_scheme, hostname, _port}} -> hostname
      _ -> nil
    end
  end

  def read_host(_backend, _label), do: nil

  @doc """
  Resolves the read connection pool size for a labeled read cluster.

  The unlabeled pool and the pool for the cluster named by `default_read_cluster`
  are sized by `read_pool_size`. Both act as the catch-all for callers that do not
  resolve to a read cluster of their own, and the default cluster additionally
  absorbs failover traffic, so they need more capacity than a single caller's
  cluster. Every other labeled pool is sized by `labeled_read_pool_size`.

  A backend-wide value wins over the application default.
  """
  @spec read_pool_size(map(), String.t() | nil) :: pos_integer()
  def read_pool_size(config, label) when is_map(config) do
    config
    |> pool_size_key(label)
    |> resolve_pool_size(config)
  end

  @impl true
  def init({backend_id, label}) do
    Process.set_label(
      {:ch_read_pool_manager, backend_id, ClickHouseAdaptor.read_cluster_tag(label)}
    )

    resolve_timer_ref = resolve_timer_send_after()

    initial_state = %__MODULE__{
      backend_id: backend_id,
      label: label,
      resolve_timer_ref: resolve_timer_ref
    }

    {:ok, initial_state}
  end

  @impl true
  def handle_call(:ensure_pool, _from, %__MODULE__{} = state) do
    case ensure_pool(state) do
      {:ok, new_state} -> {:reply, {:ok, new_state.pool_pid}, new_state}
      {error, new_state} -> {:reply, error, new_state}
    end
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
      {:reply, {:error, :no_pool}, clear_pool(state)}
    end
  end

  def handle_call(:recycle_pool, _from, %__MODULE__{} = state) do
    {:reply, {:error, :no_pool}, state}
  end

  @impl true
  def handle_call({:refresh_pool, backend}, _from, %__MODULE__{pool_pid: pool_pid} = state)
      when is_pid(pool_pid) do
    cond do
      not Process.alive?(pool_pid) ->
        {:reply, :ok, clear_pool(state)}

      pool_opts_current?(state, backend) ->
        {:reply, :ok, state}

      true ->
        GenServer.stop(pool_pid)
        {:reply, :ok, clear_pool(state)}
    end
  end

  def handle_call({:refresh_pool, _backend}, _from, %__MODULE__{} = state) do
    {:reply, :ok, clear_pool(state)}
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
      host: connection_host(state.backend_id, state.label)
    )

    {:noreply, clear_pool(state)}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @spec record_activity(__MODULE__.t()) :: __MODULE__.t()
  defp record_activity(%__MODULE__{} = state) do
    %{state | last_activity: System.system_time(:millisecond)}
  end

  @spec ensure_pool(__MODULE__.t()) :: {:ok | {:error, term()}, __MODULE__.t()}
  defp ensure_pool(%__MODULE__{pool_pid: pool_pid} = state) when is_pid(pool_pid) do
    if Process.alive?(pool_pid), do: {:ok, record_activity(state)}, else: start_pool(state)
  end

  defp ensure_pool(%__MODULE__{} = state), do: start_pool(state)

  @spec start_pool(__MODULE__.t()) :: {:ok | {:error, term()}, __MODULE__.t()}
  defp start_pool(%__MODULE__{backend_id: backend_id, label: label} = state) do
    backend = Backends.Cache.get_backend(backend_id)

    with {:ok, ch_opts} <- build_ch_opts(backend, label),
         {:ok, pid} <- Ch.start_link(ch_opts) do
      Process.monitor(pid)

      Logger.info("Started ClickHouse connection pool",
        backend_id: state.backend_id
      )

      new_state =
        %__MODULE__{
          state
          | pool_pid: pid,
            pool_size: Keyword.fetch!(ch_opts, :pool_size),
            pool_opts_hash: pool_opts_hash(ch_opts)
        }
        |> publish_pool()
        |> record_activity()
        |> schedule_next_recycle(System.system_time(:millisecond))

      {:ok, new_state}
    else
      {:error, reason} ->
        Logger.error(
          "Failed to start ClickHouse connection pool",
          backend_id: backend_id,
          host: read_host(backend, label),
          reason: reason
        )

        {{:error, reason}, state}
    end
  end

  @spec clear_pool(__MODULE__.t()) :: __MODULE__.t()
  defp clear_pool(%__MODULE__{} = state) do
    publish_pool(%__MODULE__{
      state
      | pool_pid: nil,
        pool_size: nil,
        pool_opts_hash: nil,
        next_recycle_at: nil
    })
  end

  @spec publish_pool(__MODULE__.t()) :: __MODULE__.t()
  defp publish_pool(%__MODULE__{pool_pid: nil} = state), do: put_registry_value(state, nil)

  defp publish_pool(%__MODULE__{pool_pid: pool_pid, pool_size: pool_size} = state),
    do: put_registry_value(state, %{pool: pool_pid, pool_size: pool_size})

  @spec put_registry_value(__MODULE__.t(), map() | nil) :: __MODULE__.t()
  defp put_registry_value(%__MODULE__{backend_id: backend_id, label: label} = state, value) do
    {:via, Registry, {BackendRegistry, key}} = Backends.via_backend(backend_id, __MODULE__, label)
    Registry.update_value(BackendRegistry, key, fn _value -> value end)
    state
  end

  @spec pool_opts_current?(__MODULE__.t(), Backend.t() | nil) :: boolean()
  defp pool_opts_current?(%__MODULE__{label: label} = state, backend) do
    case build_ch_opts(backend, label) do
      {:ok, ch_opts} -> pool_opts_hash(ch_opts) == state.pool_opts_hash
      {:error, _reason} -> false
    end
  end

  @spec pool_opts_hash(Keyword.t()) :: binary()
  defp pool_opts_hash(ch_opts),
    do: :crypto.hash(:sha256, :erlang.term_to_binary(ch_opts, [:deterministic]))

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
        clear_pool(state)

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

    clear_pool(state)
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

  @spec build_ch_opts(Backend.t() | nil, String.t() | nil) ::
          {:ok, Keyword.t()} | {:error, term()}
  defp build_ch_opts(nil, _label), do: {:error, :backend_not_found}

  defp build_ch_opts(%Backend{} = backend, label) do
    config = backend.config
    pool_size = read_pool_size(config, label)
    url = read_url(config, label)

    with {:ok, {scheme, hostname, url_port}} <- extract_url_components(url) do
      port = get_read_port(config, url_port)
      {username, password} = ClickHouseAdaptor.query_credentials(config)

      ch_opts = [
        scheme: scheme,
        hostname: hostname,
        port: port,
        database: config.database,
        username: username,
        password: password,
        pool_size: pool_size,
        settings: [],
        timeout: @ch_query_conn_timeout,
        queue_target: @ch_queue_target,
        idle_interval: @ch_idle_interval,
        idle_limit: pool_size,
        connection_listeners: {[@telemetry_listener], {backend.id, label}}
      ]

      {:ok, ch_opts}
    end
  end

  @spec pool_size_key(map(), String.t() | nil) :: :read_pool_size | :labeled_read_pool_size
  defp pool_size_key(_config, label) when not is_non_empty_binary(label), do: :read_pool_size

  defp pool_size_key(config, label) do
    case Map.get(config, :default_read_cluster) do
      ^label -> :read_pool_size
      _ -> :labeled_read_pool_size
    end
  end

  @spec resolve_pool_size(atom(), map()) :: pos_integer()
  defp resolve_pool_size(key, config) do
    case validate_pool_size(Map.get(config, key)) do
      nil -> default_pool_size(key)
      size -> size
    end
  end

  @spec default_pool_size(atom()) :: pos_integer()
  defp default_pool_size(:read_pool_size), do: @default_read_pool_size
  defp default_pool_size(:labeled_read_pool_size), do: @default_labeled_read_pool_size

  @spec validate_pool_size(term()) :: pos_integer() | nil
  defp validate_pool_size(size) when is_pos_integer(size), do: size
  defp validate_pool_size(_size), do: nil

  @spec read_url(map(), String.t() | nil) :: String.t() | nil
  defp read_url(config, label) do
    urls = Map.get(config, :read_only_urls) || %{}
    default = Map.get(config, :default_read_cluster)

    labeled_url(urls, label) || labeled_url(urls, default) || Map.get(config, :url)
  end

  @spec labeled_url(map(), String.t() | nil) :: String.t() | nil
  defp labeled_url(urls, label) when is_map(urls) and is_non_empty_binary(label) do
    case Map.get(urls, label) do
      url when is_non_empty_binary(url) -> url
      _ -> nil
    end
  end

  defp labeled_url(_urls, _label), do: nil

  @spec connection_host(pos_integer(), String.t() | nil) :: String.t() | nil
  defp connection_host(backend_id, label) do
    backend_id
    |> Backends.Cache.get_backend()
    |> read_host(label)
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

  @spec connection_manager_via(Backend.t(), String.t() | nil) :: tuple()
  defp connection_manager_via(%Backend{} = backend, label) do
    Backends.via_backend(backend, __MODULE__, label)
  end

  defp resolve_timer_send_after do
    resolve_interval =
      Application.get_env(:logflare, __MODULE__)[:resolve_interval] || @resolve_interval

    Process.send_after(self(), :resolve_connections, resolve_interval)
  end
end
