defmodule Logflare.AlertIssues.NotificationWorker do
  @moduledoc """
  Delivers one alert issue notification event to the control plane and records the
  outcome in `system.alert_issue_notifications`. Failed deliveries are retried with
  Oban's backoff; only the final failed attempt is recorded.
  """

  use Oban.Worker,
    queue: :default,
    max_attempts: 5,
    unique: [keys: [:dedupe_key], period: 86_400, states: Oban.Job.states()]

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.Notifier
  alias Logflare.Backends

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"user_id" => user_id, "event" => event}} = job) do
    if Notifier.enabled?() do
      deliver(user_id, event, job)
    else
      {:cancel, :notifier_disabled}
    end
  end

  defp deliver(user_id, event, %Oban.Job{attempt: attempt, max_attempts: max_attempts}) do
    case Notifier.deliver(event) do
      {:ok, status} ->
        log(user_id, event, %{"status" => "sent", "attempts" => attempt, "http_status" => status})
        :ok

      {:error, reason} = error ->
        if attempt >= max_attempts do
          log(user_id, event, %{
            "status" => "failed",
            "attempts" => attempt,
            "http_status" => http_status(reason),
            "error" => inspect(reason)
          })
        end

        error
    end
  end

  defp log(user_id, event, outcome) do
    version = System.os_time(:microsecond)
    issue = event["alert_issue"]

    row =
      Map.merge(
        %{
          "event_id" => event["event_id"],
          "alert_issue_id" => issue["alert_issue_id"],
          "alert_issue_version" => issue["version"],
          "policy_name" => get_in(event, ["policy", "name"]),
          "type" => event["type"],
          "receivers" => get_in(event, ["policy", "receivers"]),
          "http_status" => nil,
          "error" => nil,
          "version" => version,
          "timestamp" => version,
          "event_message" => "#{event["type"]} #{outcome["status"]}"
        },
        outcome
      )

    Backends.ingest_logs([row], AlertIssues.system_source(user_id, :alert_issue_notifications))
  end

  defp http_status({:http_status, status}), do: status
  defp http_status(_reason), do: nil
end
