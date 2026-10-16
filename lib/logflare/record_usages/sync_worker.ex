defmodule Logflare.RecordUsages.SyncWorker do
  @moduledoc """
  Parent job schedules one child job per node to write cached usage to the database.
  It spaces the scheduled start times across the next five minutes.

  Keep `@sync_interval_ms` no greater than the cron interval in `config/runtime.exs`.
  Cron parents have up to three attempts. Child jobs have one attempt.
  A retry can create duplicate child jobs if the database accepted the previous insert
  but did not confirm it.

  The RPC timeout ends the child's wait. It does not stop the node's flush.
  The node can still write and acknowledge usage after Oban discards the child job.
  """

  use Oban.Worker, queue: :default, max_attempts: 1, unique: [period: 60]

  alias Logflare.Cluster.Utils
  alias Logflare.RecordUsages

  @sync_interval_ms 300_000
  @rpc_timeout_ms 60_000

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:cancel, :node_unavailable}
  def perform(%Oban.Job{args: %{"node" => name}}) do
    case Enum.find(Utils.node_list_all(), &(Atom.to_string(&1) == name)) do
      nil -> {:cancel, :node_unavailable}
      node -> Utils.rpc_call(node, &RecordUsages.flush/0, @rpc_timeout_ms)
    end
  end

  def perform(%Oban.Job{}) do
    now = DateTime.utc_now()
    nodes = Enum.sort(Utils.node_list_all())
    node_count = length(nodes)

    nodes
    |> Enum.with_index()
    |> Enum.map(fn {node, index} ->
      scheduled_at = DateTime.add(now, div(index * @sync_interval_ms, node_count), :millisecond)
      new(%{"node" => Atom.to_string(node)}, scheduled_at: scheduled_at)
    end)
    |> Oban.insert_all()

    :ok
  end
end
