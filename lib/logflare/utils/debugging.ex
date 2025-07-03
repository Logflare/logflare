defmodule Logflare.Utils.Debugging do
  @moduledoc false
  alias Logflare.Backends.IngestEventQueue

  def list_counts(source_id) do
    :erpc.multicall(all_nodes(), __MODULE__, :list_counts_callback, [source_id], 5000)
  end

  def list_counts_callback(source_id) do
    {Node.self(), IngestEventQueue.list_counts({source_id, nil})}
  end

  def list_pending_counts(source_id) do
    :erpc.multicall(all_nodes(), __MODULE__, :list_pending_counts_callback, [source_id], 5000)
  end

  defp all_nodes, do: [Node.self() | Node.list()]

  def list_pending_counts_callback(source_id) do
    {Node.self(), IngestEventQueue.list_pending_counts({source_id, nil})}
  end

  @doc """
  If scheduler is started on the node, it will return the job count.
  If not started, it will raise an error.
  """
  def list_all_scheduler_job_counts do
    :erpc.multicall(all_nodes(), __MODULE__, :list_all_scheduler_job_counts_callback, [], 5000)
  end

  defp list_all_scheduler_job_counts_callback do
    jobs = Logflare.Alerting.AlertsScheduler.jobs()
    {Node.self(), jobs |> length()}
  end
end
