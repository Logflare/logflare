defmodule Logflare.Sources.CleanupWorker do
  @moduledoc """
  Retries source teardown after metadata deletion commits.

  Job arguments contain only resource identifiers, never credentials. Teardown
  acknowledges all currently connected nodes before deleting the legacy per-source
  BigQuery table. Shared backend resources are not deleted.
  """
  use Oban.Worker, queue: :default, max_attempts: 20

  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Cluster
  alias Logflare.ContextCache
  alias Logflare.Google.BigQuery
  alias Logflare.Rules
  alias Logflare.SourceSchemas
  alias Logflare.Sources
  alias Logflare.Sources.Counters
  alias Logflare.Sources.Source

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:error, term()}
  def perform(%Oban.Job{args: args}) do
    with :ok <- cleanup_cluster(args) do
      delete_table(args["bigquery"])
    end
  end

  @doc """
  Invalidates cached source configuration and stops the local tree without a DB lookup.
  """
  @spec cleanup_local(map()) :: :ok | {:error, term()}
  def cleanup_local(%{"source_id" => id, "source_token" => token} = args) do
    {:ok, uuid} = Ecto.UUID.cast(token)
    {:ok, token} = Ecto.UUID.Atom.cast(uuid)

    cache_keys = [{Sources, id}, {Rules, source_id: id}]

    cache_keys =
      if schema_id = args["source_schema_id"],
        do: [{SourceSchemas, schema_id} | cache_keys],
        else: cache_keys

    ContextCache.bust_keys(cache_keys)

    for fun <- [:get_by, :get_by_and_preload], kw <- [[id: id], [token: token]] do
      {:ok, true} = Cachex.put(Sources.Cache, {fun, [kw]}, {:cached, nil})
    end

    Backends.clear_list_backends_cache(id)
    {:ok, true} = Cachex.del(Backends.Cache, {:list_backends, [[rules_source_id: id]]})

    with :ok <- stop_source(id) do
      {:ok, _} = Counters.delete(token)
      :ok
    end
  end

  @spec cleanup_cluster(map()) :: :ok | {:error, term()}
  defp cleanup_cluster(args) do
    failures =
      Cluster.Utils.node_list_all()
      |> Cluster.Utils.erpc_multicall(__MODULE__, :cleanup_local, [args], 30_000)
      |> Enum.reject(fn {_node, result} -> result == {:ok, :ok} end)

    case failures do
      [] -> :ok
      failures -> {:error, {:cluster_cleanup_failed, failures}}
    end
  end

  @spec stop_source(pos_integer()) :: :ok | {:error, term()}
  defp stop_source(id) do
    case Backends.lookup(SourceSup, %Source{id: id}) do
      {:ok, pid} ->
        supervisor = {:via, PartitionSupervisor, {Backends.SourcesSup, id}}

        case DynamicSupervisor.terminate_child(supervisor, pid) do
          :ok -> :ok
          {:error, :not_found} -> :ok
        end

      {:error, :not_started} ->
        :ok
    end
  end

  @spec delete_table(map() | nil) :: :ok | {:error, term()}
  defp delete_table(nil), do: :ok

  defp delete_table(%{"project_id" => project, "dataset_id" => dataset, "table_id" => table}) do
    case BigQuery.delete_table(project, dataset, table) do
      {:ok, _} -> :ok
      {:error, %Tesla.Env{status: 404}} -> :ok
      {:error, %Tesla.Env{status: status}} -> {:error, {:bigquery_delete_failed, status}}
      {:error, reason} when is_atom(reason) -> {:error, {:bigquery_delete_failed, reason}}
    end
  end
end
