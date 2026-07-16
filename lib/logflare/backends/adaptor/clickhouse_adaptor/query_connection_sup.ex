defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryConnectionSup do
  @moduledoc """
  This supervisor only manages read/query `ConnectionManager` instances. Write/ingest
  traffic does not flow through these pools — HTTP inserts use the
  `Logflare.FinchClickHouseIngest` Finch pool and native protocol inserts use the
  `NativeIngester` connection pools.

  ## Supervision Hierarchy

  ```
  Logflare.Backends.Supervisor
  └── QueryConnectionSup (this module)
      └── DynamicSupervisor
          └── ConnectionManager (one per backend, lazily started)
  ```

  ## Why The Split Between Reads/Writes?

  1. **Source independence** - Read queries should work even if no source is actively ingesting
  2. **Shared resources** - Multiple sources using the same backend share a single read pool
  3. **Persistence** - Read pools should survive source restarts
  4. **Separation of concerns** - Reads and writes have different performance characteristics

  ## Monitoring

  Use `count_query_connection_managers/0` to monitor how many backends currently have
  active read connection pools.

  `list_query_connection_managers/0` can be used to retrieve a list of all active read
  connection manager PIDs along with their respective backend IDs.

  ## Modifying Pools

  `recycle_backend/1` triggers an immediate connection recycle of a backend's read pool
  on every node in the cluster. Useful after scaling an upstream ClickHouse service.

  `refresh_backend/1` stops a backend's read pool on every node so the pools restart
  with freshly loaded backend configuration.

  `terminate_backend/1` terminates a backend's read `ConnectionManager` on every node,
  e.g. after a backend is deleted.
  """

  use Supervisor

  import Logflare.Utils.Guards

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.ConnectionManager
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BackendRegistry
  alias Logflare.Cluster
  alias Logflare.ContextCache

  @dynamic_sup_name __MODULE__.DynamicSupervisor
  @recycle_rpc_timeout :timer.seconds(15)
  @recycle_chunk_size 25
  @recycle_chunk_delay :timer.seconds(1)

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
  Starts a query `ConnectionManager` as a supervised child.

  Returns `{:ok, pid}` on success or `{:error, reason}` on failure.
  Handles the case where the `ConnectionManager` is already started.
  """
  @spec start_connection_manager(Supervisor.child_spec()) ::
          DynamicSupervisor.on_start_child()
  def start_connection_manager(child_spec) do
    DynamicSupervisor.start_child(@dynamic_sup_name, child_spec)
  end

  @doc """
  Returns the count of currently running query `ConnectionManager` processes.
  """
  @spec count_query_connection_managers() :: non_neg_integer()
  def count_query_connection_managers do
    @dynamic_sup_name
    |> DynamicSupervisor.which_children()
    |> length()
  end

  @doc """
  Returns all currently running query `ConnectionManager` PIDs and their backend IDs.
  """
  @spec list_query_connection_managers() :: [{backend_id :: integer(), pid()}]
  def list_query_connection_managers do
    Registry.select(BackendRegistry, [
      {{{ConnectionManager, :"$1"}, :"$2", :_}, [], [{{:"$1", :"$2"}}]},
      {{{ConnectionManager, :"$1", :_}, :"$2", :_}, [], [{{:"$1", :"$2"}}]}
    ])
  end

  @doc """
  Recycles the read connection pool for a backend on every node in the cluster.

  Useful after scaling an upstream ClickHouse service: rather than waiting for each
  pool's scheduled recycle, the backend's read connections begin re-establishing right
  away so a least-connections load balancer can redistribute them across replicas.

  The fan-out is staggered in node chunks to avoid reconnecting the entire cluster at once.
  Returns the result of the recycle on each node, keyed by node.

  Note that this call does block while waiting to dispatch to all nodes.
  """
  @spec recycle_backend(Backend.t() | pos_integer()) :: %{node() => :ok | {:error, term()}}
  def recycle_backend(%Backend{id: backend_id}), do: recycle_backend(backend_id)

  def recycle_backend(backend_id) when is_pos_integer(backend_id) do
    Logger.info("Recycling ClickHouse read pool connections across the cluster",
      backend_id: backend_id
    )

    Cluster.Utils.node_list_all()
    |> Enum.chunk_every(@recycle_chunk_size)
    |> Enum.with_index()
    |> Enum.flat_map(fn {nodes, index} ->
      if index > 0, do: Process.sleep(@recycle_chunk_delay)

      Cluster.Utils.erpc_multicall(
        nodes,
        __MODULE__,
        :recycle_backend_local,
        [backend_id],
        @recycle_rpc_timeout
      )
    end)
    |> Map.new(fn
      {node, {:ok, result}} -> {node, result}
      {node, {:error, reason}} -> {node, {:error, reason}}
      {node, {class, reason}} when class in [:throw, :exit] -> {node, {:error, {class, reason}}}
    end)
  end

  @doc """
  Recycles the read connection pool for a backend on the local node.

  Invoked on each node by `recycle_backend/1`.
  """
  @spec recycle_backend_local(pos_integer()) :: :ok | {:error, term()}
  def recycle_backend_local(backend_id) when is_pos_integer(backend_id) do
    case lookup_managers(backend_id) do
      [] -> {:error, :no_manager}
      manager_pids -> manager_pids |> Enum.map(&safe_recycle/1) |> aggregate_results()
    end
  end

  @spec safe_recycle(pid()) :: :ok | {:error, term()}
  defp safe_recycle(manager_pid) do
    ConnectionManager.recycle_pool(manager_pid)
  catch
    :exit, _reason -> {:error, :unavailable}
  end

  @doc """
  Stops the read connection pool for a backend on every node in the cluster so that
  each node's next query restarts it with freshly loaded backend configuration.

  Pools restart lazily on demand.
  """
  @spec refresh_backend(Backend.t() | pos_integer()) :: :ok
  def refresh_backend(%Backend{id: backend_id}), do: refresh_backend(backend_id)

  def refresh_backend(backend_id) when is_pos_integer(backend_id) do
    Logger.info("Refreshing ClickHouse read pools across the cluster",
      backend_id: backend_id
    )

    Cluster.Utils.rpc_multicast(__MODULE__, :refresh_backend_local, [backend_id])
    :ok
  end

  @doc """
  Busts the local backend cache and stops the backend's read pool on the local node.

  Invoked on each node by `refresh_backend/1`.
  """
  @spec refresh_backend_local(pos_integer()) :: :ok
  def refresh_backend_local(backend_id) when is_pos_integer(backend_id) do
    ContextCache.bust_keys([{Backends, backend_id}])

    backend_id
    |> lookup_managers()
    |> Enum.each(&safe_refresh/1)

    :ok
  end

  @spec safe_refresh(pid()) :: :ok
  defp safe_refresh(manager_pid) do
    ConnectionManager.refresh_pool(manager_pid)
  catch
    :exit, _reason -> :ok
  end

  @doc """
  Terminates the read `ConnectionManager` and its pool for a backend on every node
  in the cluster. Used when a backend is deleted. Fire-and-forget.
  """
  @spec terminate_backend(Backend.t() | pos_integer()) :: :ok
  def terminate_backend(%Backend{id: backend_id}), do: terminate_backend(backend_id)

  def terminate_backend(backend_id) when is_pos_integer(backend_id) do
    Logger.info("Terminating ClickHouse read connection managers across the cluster",
      backend_id: backend_id
    )

    Cluster.Utils.rpc_multicast(__MODULE__, :terminate_backend_local, [backend_id])
    :ok
  end

  @doc """
  Terminates the backend's read `ConnectionManager` on the local node.

  Invoked on each node by `terminate_backend/1`.
  """
  @spec terminate_backend_local(pos_integer()) :: :ok
  def terminate_backend_local(backend_id) when is_pos_integer(backend_id) do
    backend_id
    |> lookup_managers()
    |> Enum.each(&terminate_manager/1)

    :ok
  end

  @spec lookup_managers(pos_integer()) :: [pid()]
  defp lookup_managers(backend_id) when is_pos_integer(backend_id) do
    Registry.select(BackendRegistry, [
      {{{ConnectionManager, backend_id}, :"$1", :_}, [], [:"$1"]},
      {{{ConnectionManager, backend_id, :_}, :"$1", :_}, [], [:"$1"]}
    ])
  end

  @spec aggregate_results([:ok | {:error, term()}]) :: :ok | {:error, term()}
  defp aggregate_results(results), do: Enum.find(results, :ok, &(&1 != :ok))

  @spec terminate_manager(pid()) :: :ok
  defp terminate_manager(manager_pid) do
    case DynamicSupervisor.terminate_child(@dynamic_sup_name, manager_pid) do
      :ok ->
        :ok

      {:error, :not_found} ->
        GenServer.stop(manager_pid, :shutdown)
        :ok
    end
  catch
    :exit, reason ->
      Logger.warning(
        "Exited while terminating ClickHouse read connection manager: #{inspect(reason)}"
      )

      :ok
  end

  @doc """
  Terminates all query `ConnectionManager` processes.
  """
  @spec terminate_all() :: :ok
  def terminate_all do
    @dynamic_sup_name
    |> DynamicSupervisor.which_children()
    |> Enum.each(fn {_id, pid, :worker, _modules} ->
      DynamicSupervisor.terminate_child(@dynamic_sup_name, pid)
    end)

    :ok
  end
end
