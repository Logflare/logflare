defmodule Logflare.Auth.AccessTokenUsageSyncWorker do
  @moduledoc false
  use Oban.Worker, queue: :default

  alias Logflare.Auth
  alias Logflare.Auth.UsageCache
  alias Logflare.Cluster.Utils

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:error, term()}
  def perform(_job) do
    failures =
      Utils.node_list_all()
      |> Utils.erpc_multicall(UsageCache, :snapshot, [])
      |> Enum.map(&flush_node/1)
      |> Enum.reject(&(&1 == :ok))

    case failures do
      [] -> :ok
      failures -> {:error, failures}
    end
  end

  @spec flush_node({node(), term()}) :: :ok | {:error, term()}
  defp flush_node({node, {:ok, entries}}) do
    entries
    |> Enum.chunk_every(1_000)
    |> Enum.reduce_while(:ok, fn batch, :ok ->
      :ok = Auth.persist_access_token_usage(batch)

      case Utils.erpc_multicall([node], UsageCache, :acknowledge, [batch]) do
        [{^node, {:ok, :ok}}] -> {:cont, :ok}
        [{^node, error}] -> {:halt, {:error, {node, error}}}
      end
    end)
  end

  defp flush_node({node, error}), do: {:error, {node, error}}
end
