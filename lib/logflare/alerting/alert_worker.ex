defmodule Logflare.Alerting.AlertWorker do
  @moduledoc false
  use Oban.Worker,
    queue: :alerts,
    max_attempts: 1,
    unique: [
      period: :infinity,
      keys: [:alert_query_id, :scheduled_at],
      states: [:available, :scheduled, :executing]
    ]

  alias Logflare.Alerting

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok | {:error, any()}
  def perform(%Oban.Job{args: %{"alert_query_id" => alert_query_id}}) do
    case Alerting.run_alert(alert_query_id, :scheduled) do
      :ok -> :ok
      {:error, :not_enabled} -> :ok
      {:error, :no_results} -> :ok
      {:error, :not_found} -> :ok
      {:error, :below_min_cluster_size} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end
end
