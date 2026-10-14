defmodule Logflare.RecordUsages.SyncWorker do
  @moduledoc false
  use Oban.Worker, queue: :default

  alias Logflare.Cluster.Utils
  alias Logflare.RecordUsages
  alias Logflare.RecordUsages.Cache

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:error, term()}
  def perform(_job) do
    {snapshots, unavailable} =
      Utils.node_list_all()
      |> Utils.erpc_multicall(Cache, :snapshot, [])
      |> Enum.split_with(fn {_node, result} -> match?({:ok, _entries}, result) end)

    snapshots
    |> merge_snapshots()
    |> Enum.chunk_every(1_000)
    |> Enum.each(fn batch -> :ok = RecordUsages.persist(batch) end)

    snapshot_failures = Enum.map(unavailable, &{:error, &1})

    acknowledgement_failures =
      snapshots
      |> Enum.map(&acknowledge_node/1)
      |> Enum.reject(&(&1 == :ok))

    case snapshot_failures ++ acknowledgement_failures do
      [] -> :ok
      failures -> {:error, failures}
    end
  end

  @spec merge_snapshots([{node(), {:ok, Cache.snapshot()}}]) :: Cache.snapshot()
  defp merge_snapshots(snapshots) do
    snapshots
    |> Enum.reduce(%{}, fn {_node, {:ok, entries}}, merged ->
      Enum.reduce(entries, merged, fn {key, timestamp}, acc ->
        Map.update(acc, key, timestamp, &max(&1, timestamp))
      end)
    end)
    |> Map.to_list()
  end

  @spec acknowledge_node({node(), {:ok, Cache.snapshot()}}) :: :ok | {:error, term()}
  defp acknowledge_node({node, {:ok, entries}}) do
    entries
    |> Enum.chunk_every(1_000)
    |> Enum.reduce_while(:ok, fn batch, :ok ->
      case Utils.erpc_multicall([node], Cache, :acknowledge, [batch]) do
        [{^node, {:ok, :ok}}] -> {:cont, :ok}
        [{^node, error}] -> {:halt, {:error, {node, error}}}
      end
    end)
  end
end
