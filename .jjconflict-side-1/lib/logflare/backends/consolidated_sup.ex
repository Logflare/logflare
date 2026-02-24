defmodule Logflare.Backends.ConsolidatedSup do
  @moduledoc """
  Supervises consolidated backend pipelines.

  Each consolidated backend gets a single adaptor supervisor that handles
  events from all sources using that backend. This enables larger batch
  sizes and more efficient writes compared to per-source pipelines.

  ## Supervision Hierarchy

  ```
  Logflare.Backends.Supervisor
  └── ConsolidatedSup (this module)
      ├── ConsolidatedSupWorker (reconciliation)
      └── DynamicSupervisor
          └── Adaptor (e.g., ClickHouseAdaptor)
  ```
  """

  use Supervisor

  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.ConsolidatedSupWorker

  @dynamic_sup_name __MODULE__.DynamicSupervisor

  def start_link(args) do
    Supervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  @impl Supervisor
  def init(_args) do
    children = [
      {DynamicSupervisor, strategy: :one_for_one, name: @dynamic_sup_name},
      ConsolidatedSupWorker
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc """
  Starts a consolidated pipeline for a backend.
  """
  @spec start_pipeline(Backend.t()) :: DynamicSupervisor.on_start_child()
  def start_pipeline(%Backend{} = backend) do
    adaptor_module = Adaptor.get_adaptor(backend)
    child_spec = adaptor_module.child_spec(backend)
    DynamicSupervisor.start_child(@dynamic_sup_name, child_spec)
  end

  @doc """
  Stops a consolidated pipeline for a backend.
  """
  @spec stop_pipeline(Backend.t() | pos_integer()) :: :ok | {:error, :not_found}
  def stop_pipeline(%Backend{} = backend) do
    case find_pipeline_pid(backend) do
      nil ->
        {:error, :not_found}

      pid ->
        DynamicSupervisor.terminate_child(@dynamic_sup_name, pid)
    end
  end

  def stop_pipeline(backend_id) when is_integer(backend_id) do
    case find_pipeline_pid_by_backend_id(backend_id) do
      nil ->
        {:error, :not_found}

      pid ->
        DynamicSupervisor.terminate_child(@dynamic_sup_name, pid)
    end
  end

  @doc """
  Checks if a consolidated pipeline is running for a backend.
  """
  @spec pipeline_running?(Backend.t() | pos_integer()) :: boolean()
  def pipeline_running?(%Backend{} = backend) do
    find_pipeline_pid(backend) != nil
  end

  def pipeline_running?(backend_id) when is_integer(backend_id) do
    case Backends.Cache.get_backend(backend_id) do
      nil -> false
      backend -> pipeline_running?(backend)
    end
  end

  @doc """
  Returns the count of currently running consolidated pipelines.
  """
  @spec count_pipelines() :: non_neg_integer()
  def count_pipelines do
    @dynamic_sup_name
    |> DynamicSupervisor.which_children()
    |> length()
  end

  @doc """
  Returns all currently running consolidated pipeline PIDs and their backend IDs.
  """
  @spec list_pipelines() :: [{backend_id :: integer(), pid()}]
  def list_pipelines do
    @dynamic_sup_name
    |> DynamicSupervisor.which_children()
    |> Enum.map(&extract_backend_info/1)
    |> Enum.reject(&is_nil/1)
  end

  defp find_pipeline_pid(%Backend{} = backend) do
    adaptor_module = Adaptor.get_adaptor(backend)
    via = Backends.via_backend(backend, adaptor_module)

    case GenServer.whereis(via) do
      pid when is_pid(pid) -> pid
      nil -> nil
    end
  end

  defp find_pipeline_pid_by_backend_id(backend_id) when is_integer(backend_id) do
    list_pipelines()
    |> Enum.find_value(fn
      {^backend_id, pid} -> pid
      _ -> nil
    end)
  end

  defp extract_backend_info({:undefined, pid, _type, _modules}) when is_pid(pid) do
    case Registry.keys(Backends.BackendRegistry, pid) do
      [{_module, backend_id}] -> {backend_id, pid}
      _ -> nil
    end
  end

  defp extract_backend_info(_child), do: nil
end
