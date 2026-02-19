defmodule Logflare.FetchQueries.FetchQuerySchedulerWorker do
  @moduledoc """
  Oban worker that runs periodically to pre-populate the fetch query queue.
  Scheduled to run every 5 minutes via Oban.Plugins.Cron.
  """

  use Oban.Worker,
    queue: :default,
    max_attempts: 1

  alias Logflare.FetchQueries
  alias Logflare.FetchQueries.FetchQueryWorker

  require Logger

  @impl Oban.Worker
  def perform(%Oban.Job{}) do
    fetch_queries = FetchQueries.list_enabled_fetch_queries()

    Logger.info("Populating fetch query queue",
      count: length(fetch_queries)
    )

    for fetch_query <- fetch_queries do
      schedule_upcoming_jobs(fetch_query)
    end

    :ok
  end

  defp schedule_upcoming_jobs(fetch_query) do
    with {:ok, cron_expr} <- Crontab.CronExpression.Parser.parse(fetch_query.cron) do
      now = DateTime.utc_now()

      # Calculate next 10 run times
      next_runs =
        Crontab.Scheduler.get_next_run_dates(cron_expr, now)
        |> Enum.take(10)

      for scheduled_at <- next_runs do
        %{
          fetch_query_id: fetch_query.id,
          scheduled_at: DateTime.to_iso8601(scheduled_at)
        }
        |> FetchQueryWorker.new(scheduled_at: scheduled_at)
        |> Oban.insert()
      end

      :ok
    else
      {:error, reason} ->
        Logger.error("Failed to schedule fetch query",
          fetch_query_id: fetch_query.id,
          reason: inspect(reason)
        )

        :ok
    end
  end
end
