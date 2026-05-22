defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup do
  @moduledoc """
  Supervises `NativeIngester.Pool` and `NativeIngester.PoolManager` instances
  for ClickHouse backends that have the native TCP insert protocol enabled.

  Pools are started lazily on the first native insert. Each pool has a
  `PoolManager` that tracks activity and stops the pool after 5 minutes
  of inactivity.

  ## Supervision Hierarchy

  ```
  Logflare.Backends.Supervisor
  └── NativeIngester.PoolSup (this module)
      ├── PoolDynamicSupervisor       (manages Pool instances, one per backend per index)
      ├── ManagerDynamicSupervisor    (manages PoolManager instances)
      └── ScalerDynamicSupervisor     (manages PoolScaler instances)
  ```
  """

  use Supervisor

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolManager
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolScaler
  alias Logflare.Backends.Backend

  @pool_sup_name __MODULE__.PoolDynamicSupervisor
  @manager_sup_name __MODULE__.ManagerDynamicSupervisor
  @scaler_sup_name __MODULE__.ScalerDynamicSupervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: @pool_sup_name},
      {DynamicSupervisor, strategy: :one_for_one, name: @manager_sup_name},
      {DynamicSupervisor, strategy: :one_for_one, name: @scaler_sup_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Ensures a native connection pool is running for the given backend.

  Starts a `PoolManager` and `PoolScaler` if not already running, then delegates to
  `PoolManager.ensure_pool_started/1` which starts pool 0 and records activity.
  """
  @spec ensure_started(Backend.t()) :: :ok | {:error, term()}
  def ensure_started(%Backend{} = backend) do
    with :ok <- ensure_manager_started(backend),
         :ok <- ensure_scaler_started(backend) do
      PoolManager.ensure_pool_started(backend)
    end
  end

  @doc """
  Starts native connection pool 0 as a supervised child.
  """
  @spec start_pool(Backend.t()) :: :ok | {:error, term()}
  def start_pool(%Backend{} = backend) do
    start_pool(backend, 0)
  end

  @doc """
  Starts a native connection pool at `index` as a supervised child.
  """
  @spec start_pool(Backend.t(), non_neg_integer()) :: :ok | {:error, term()}
  def start_pool(%Backend{} = backend, index) when is_integer(index) and index >= 0 do
    case DynamicSupervisor.start_child(@pool_sup_name, Pool.child_spec({backend, index})) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stops pool 0 for the given backend, if running.
  """
  @spec stop_pool(Backend.t()) :: :ok
  def stop_pool(%Backend{} = backend) do
    stop_pool(backend, 0)
  end

  @doc """
  Stops the pool at `index` for the given backend, if running.
  """
  @spec stop_pool(Backend.t(), non_neg_integer()) :: :ok
  def stop_pool(%Backend{} = backend, index) when is_integer(index) and index >= 0 do
    case GenServer.whereis(Pool.via(backend, index)) do
      nil ->
        :ok

      pid ->
        DynamicSupervisor.terminate_child(@pool_sup_name, pid)
        :ok
    end
  end

  @doc """
  Stops the pool manager for the given backend, if running.
  """
  @spec stop_manager(Backend.t()) :: :ok
  def stop_manager(%Backend{} = backend) do
    via = Logflare.Backends.via_backend(backend, PoolManager)

    case GenServer.whereis(via) do
      nil ->
        :ok

      pid ->
        DynamicSupervisor.terminate_child(@manager_sup_name, pid)
        :ok
    end
  end

  @doc """
  Stops the pool scaler for the given backend, if running.
  """
  @spec stop_scaler(Backend.t()) :: :ok
  def stop_scaler(%Backend{} = backend) do
    case GenServer.whereis(PoolScaler.via(backend)) do
      nil ->
        :ok

      pid ->
        DynamicSupervisor.terminate_child(@scaler_sup_name, pid)
        :ok
    end
  end

  @doc """
  Returns the count of currently running native connection pools.
  """
  @spec count_pools() :: non_neg_integer()
  def count_pools do
    @pool_sup_name
    |> DynamicSupervisor.which_children()
    |> length()
  end

  @spec ensure_manager_started(Backend.t()) :: :ok | {:error, term()}
  defp ensure_manager_started(%Backend{} = backend) do
    via = Logflare.Backends.via_backend(backend, PoolManager)

    case GenServer.whereis(via) do
      nil ->
        case DynamicSupervisor.start_child(@manager_sup_name, PoolManager.child_spec(backend)) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, reason} -> {:error, reason}
        end

      _pid ->
        :ok
    end
  end

  @spec ensure_scaler_started(Backend.t()) :: :ok | {:error, term()}
  defp ensure_scaler_started(%Backend{} = backend) do
    case GenServer.whereis(PoolScaler.via(backend)) do
      nil ->
        case DynamicSupervisor.start_child(@scaler_sup_name, PoolScaler.child_spec(backend)) do
          {:ok, _pid} -> :ok
          {:error, {:already_started, _pid}} -> :ok
          {:error, reason} -> {:error, reason}
        end

      _pid ->
        :ok
    end
  end
end
