defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolManager do
  @moduledoc """
  Manages the lifecycle of a native TCP connection pool for a single ClickHouse backend.

  Tracks insert activity and automatically stops the pool after a period of
  inactivity, freeing resources for backends that are no longer receiving events.
  """

  use GenServer
  use TypedStruct

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup
  alias Logflare.Backends.Backend

  @inactivity_timeout :timer.minutes(5)
  @resolve_interval :timer.seconds(30)

  typedstruct do
    field :backend_id, pos_integer(), enforce: true
    field :pool_pid, pid() | nil, default: nil
    field :last_activity, integer() | nil, default: nil
    field :resolve_timer_ref, reference() | nil, default: nil
  end

  @spec start_link(Backend.t()) :: GenServer.on_start()
  def start_link(%Backend{id: backend_id} = backend) do
    GenServer.start_link(__MODULE__, backend_id, name: via(backend))
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
  Ensures the native TCP pool is running for the given backend.

  Starts the pool if not already running and records activity.
  """
  @spec ensure_pool_started(Backend.t()) :: :ok | {:error, term()}
  def ensure_pool_started(%Backend{} = backend) do
    backend
    |> via()
    |> GenServer.call(:ensure_pool)
  end

  @doc """
  Notifies the pool manager that an insert occurred, resetting the inactivity timer.
  """
  @spec notify_activity(Backend.t()) :: :ok
  def notify_activity(%Backend{} = backend) do
    backend
    |> via()
    |> GenServer.cast(:update_activity)
  end

  @doc """
  Returns whether the native TCP pool is currently running.
  """
  @spec pool_active?(Backend.t()) :: boolean()
  def pool_active?(%Backend{} = backend) do
    backend
    |> via()
    |> GenServer.call(:pool_active)
  catch
    :exit, _ -> false
  end

  @spec via(Backend.t()) :: GenServer.name()
  defp via(%Backend{} = backend) do
    Backends.via_backend(backend, __MODULE__)
  end

  @impl true
  def init(backend_id) do
    resolve_timer_ref = Process.send_after(self(), :resolve_pool_state, @resolve_interval)

    {:ok, %__MODULE__{backend_id: backend_id, resolve_timer_ref: resolve_timer_ref}}
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
  def handle_cast(:update_activity, %__MODULE__{} = state), do: {:noreply, record_activity(state)}

  @impl true
  def handle_info(:resolve_pool_state, %__MODULE__{} = state) do
    if state.resolve_timer_ref do
      Process.cancel_timer(state.resolve_timer_ref)
    end

    new_timer_ref = Process.send_after(self(), :resolve_pool_state, @resolve_interval)

    new_state =
      %__MODULE__{state | resolve_timer_ref: new_timer_ref}
      |> resolve_pool_state()

    {:noreply, new_state}
  end

  @impl true
  def handle_info({:DOWN, _ref, :process, pid, _reason}, %__MODULE__{pool_pid: pid} = state)
      when is_pid(pid) do
    Logger.warning("ClickHouse native TCP pool died", backend_id: state.backend_id)

    {:noreply, %{state | pool_pid: nil}}
  end

  def handle_info({:DOWN, _ref, :process, _pid, _reason}, %__MODULE__{} = state) do
    {:noreply, state}
  end

  @spec record_activity(__MODULE__.t()) :: __MODULE__.t()
  defp record_activity(%__MODULE__{} = state) do
    %{state | last_activity: System.system_time(:millisecond)}
  end

  @spec start_pool(__MODULE__.t()) :: {:ok, __MODULE__.t()} | {{:error, term()}, __MODULE__.t()}
  defp start_pool(%__MODULE__{backend_id: backend_id} = state) do
    backend = Backends.Cache.get_backend(backend_id)

    if is_nil(backend) do
      {{:error, :backend_not_found}, state}
    else
      do_start_pool(backend, state)
    end
  end

  @spec do_start_pool(Backend.t(), __MODULE__.t()) ::
          {:ok, __MODULE__.t()} | {{:error, term()}, __MODULE__.t()}
  defp do_start_pool(%Backend{} = backend, %__MODULE__{backend_id: backend_id} = state) do
    case PoolSup.start_pool(backend) do
      :ok ->
        monitor_pool(backend, state)

      {:error, reason} ->
        Logger.error("Failed to start ClickHouse native TCP pool",
          backend_id: backend_id,
          reason: reason
        )

        {{:error, reason}, state}
    end
  end

  @spec monitor_pool(Backend.t(), __MODULE__.t()) ::
          {:ok, __MODULE__.t()} | {{:error, term()}, __MODULE__.t()}
  defp monitor_pool(%Backend{} = backend, %__MODULE__{backend_id: backend_id} = state) do
    case GenServer.whereis(Pool.via(backend)) do
      nil ->
        {{:error, :pool_not_found}, state}

      pid ->
        Process.monitor(pid)

        Logger.info("Started ClickHouse native TCP pool",
          backend_id: backend_id
        )

        new_state =
          %__MODULE__{state | pool_pid: pid}
          |> record_activity()

        {:ok, new_state}
    end
  end

  @spec resolve_pool_state(__MODULE__.t()) :: __MODULE__.t()
  defp resolve_pool_state(%__MODULE__{pool_pid: pool_pid, last_activity: last_activity} = state) do
    now = System.system_time(:millisecond)

    cond do
      is_nil(pool_pid) ->
        state

      not Process.alive?(pool_pid) ->
        %__MODULE__{state | pool_pid: nil}

      is_nil(last_activity) ->
        record_activity(state)

      now - last_activity < @inactivity_timeout ->
        state

      true ->
        stop_pool(state)
    end
  end

  @spec stop_pool(__MODULE__.t()) :: __MODULE__.t()
  defp stop_pool(%__MODULE__{pool_pid: pool_pid, backend_id: backend_id} = state)
       when is_pid(pool_pid) do
    backend = Backends.Cache.get_backend(backend_id)

    if backend do
      PoolSup.stop_pool(backend)

      Logger.info("Stopped ClickHouse native TCP pool due to inactivity",
        backend_id: backend_id
      )
    end

    %__MODULE__{state | pool_pid: nil}
  end
end
