defmodule Logflare.Alerting.AlertSchedulerWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Crontab.CronExpression.Parser
  alias Crontab.Scheduler
  alias Logflare.Alerting
  alias Logflare.Alerting.AlertWorker

  require Logger

  @impl Oban.Worker
  @spec perform(Oban.Job.t()) :: :ok
  def perform(_job) do
    alert_queries = Alerting.list_all_alert_queries()

    Enum.each(alert_queries, fn alert_query ->
      schedule_jobs_for_alert(alert_query)
    end)

    :ok
  end

  defp schedule_jobs_for_alert(alert_query) do
    case Parser.parse(alert_query.cron) do
      {:ok, cron_expr} ->
        now = NaiveDateTime.utc_now()

        cron_expr
        |> Scheduler.get_next_run_dates(now)
        |> Enum.take(5)
        |> Enum.each(fn run_date ->
          scheduled_at = DateTime.from_naive!(run_date, "Etc/UTC")

          %{alert_query_id: alert_query.id, scheduled_at: DateTime.to_iso8601(scheduled_at)}
          |> AlertWorker.new(scheduled_at: scheduled_at)
          |> Oban.insert()
        end)

      {:error, reason} ->
        Logger.warning("Invalid cron expression for alert #{alert_query.id}: #{inspect(reason)}")
    end
  end
end
