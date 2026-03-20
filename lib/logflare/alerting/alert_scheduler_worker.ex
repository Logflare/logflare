defmodule Logflare.Alerting.AlertSchedulerWorker do
  @moduledoc false
  use Oban.Worker, queue: :alerts, max_attempts: 3

  alias Logflare.Alerting

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok
  def perform(_job) do
    alert_queries = Alerting.list_all_alert_queries()

    Enum.each(alert_queries, fn alert_query ->
      Alerting.schedule_alert(alert_query)
    end)

    :ok
  end
end
