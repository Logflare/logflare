defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.PoolSup do
  @moduledoc """
  Supervises `NativeIngester.Pool` instances for ClickHouse backends that have
  the native TCP insert protocol enabled.

  Pools are started lazily on the first native insert and persist until the
  backend config changes or the pool is explicitly stopped.

  ## Supervision Hierarchy

  ```
  Logflare.Backends.Supervisor
  └── NativeIngester.PoolSup (this module)
      └── DynamicSupervisor
          └── NativeIngester.Pool (one per backend, lazily started)
  ```
  """

  use Supervisor

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Pool
  alias Logflare.Backends.Backend

  @dynamic_sup_name __MODULE__.DynamicSupervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl true
  def init(_args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_sup_name}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Ensures a native connection pool is running for the given backend.
  """
  @spec ensure_started(Backend.t()) :: :ok | {:error, term()}
  def ensure_started(%Backend{} = backend) do
    case GenServer.whereis(Pool.via(backend)) do
      nil -> start_pool(backend)
      _pid -> :ok
    end
  end

  @doc """
  Starts a native connection pool as a supervised child.
  """
  @spec start_pool(Backend.t()) :: :ok | {:error, term()}
  def start_pool(%Backend{} = backend) do
    case DynamicSupervisor.start_child(@dynamic_sup_name, Pool.child_spec(backend)) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Stops the native connection pool for the given backend, if running.
  """
  @spec stop_pool(Backend.t()) :: :ok
  def stop_pool(%Backend{} = backend) do
    case GenServer.whereis(Pool.via(backend)) do
      nil ->
        :ok

      pid ->
        DynamicSupervisor.terminate_child(@dynamic_sup_name, pid)
        :ok
    end
  end

  @doc """
  Returns the count of currently running native connection pools.
  """
  @spec count_pools() :: non_neg_integer()
  def count_pools do
    @dynamic_sup_name
    |> DynamicSupervisor.which_children()
    |> length()
  end
end
