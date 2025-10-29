defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.QueryConnectionSup do
  @moduledoc """
  This supervisor only manages read/query `ConnectionManager` instances. It does not handle
  write/ingest `ConnectionManager` instances, which are supervised per-source under `Logflare.Backends.SourceSup`.

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

  Use `count_connection_managers/0` to monitor how many backends currently have
  active read connection pools.

  `list_connection_managers/0` can be used to retrieve a list of all active read connection manager PIDs
  along with their respective backend IDs.
  """

  use Supervisor

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
    @dynamic_sup_name
    |> DynamicSupervisor.which_children()
    |> Enum.map(&extract_backend_info/1)
    |> Enum.reject(&is_nil/1)
  end

  @spec extract_backend_info(DynamicSupervisor.child()) :: {integer(), pid()} | nil
  defp extract_backend_info({id, pid, :worker, _modules}) when is_tuple(id) do
    case id do
      {_module, {backend_id}} -> {backend_id, pid}
      _ -> nil
    end
  end

  defp extract_backend_info(_child), do: nil

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
