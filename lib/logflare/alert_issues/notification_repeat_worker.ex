defmodule Logflare.AlertIssues.NotificationRepeatWorker do
  @moduledoc """
  Every minute, sends `repeat` events for unresolved alert issues whose last delivery
  for a repeating policy is older than its `repeat_interval_seconds` (or, with no
  delivery yet, whose `attention_at` is). Deliveries are read from
  `system.alert_issue_notifications`, so repeats stay correct across restarts.
  """

  use Oban.Worker, queue: :default, max_attempts: 1

  alias Logflare.AlertIssues.NotificationPolicies
  alias Logflare.AlertIssues.Notifications
  alias Logflare.AlertIssues.Notifier
  alias Logflare.AlertIssues.Queries
  alias Logflare.Users

  @impl Oban.Worker
  def perform(_job) do
    if Notifier.enabled?() do
      NotificationPolicies.list_repeating()
      |> Enum.group_by(& &1.user_id)
      |> Enum.each(fn {user_id, policies} -> repeat_for_user(user_id, policies) end)
    end

    :ok
  end

  defp repeat_for_user(user_id, policies) do
    now_us = System.os_time(:microsecond)

    with %_{} = user <- Users.get(user_id),
         {:ok, issues} <- Queries.list_active(user),
         {:ok, deliveries} <-
           Queries.last_notifications(user, Enum.map(issues, & &1["alert_issue_id"])) do
      for issue <- issues, policy <- policies, due?(issue, policy, deliveries, now_us) do
        interval_us = policy.repeat_interval_seconds * 1_000_000

        Notifications.notify(user_id, "repeat", issue,
          policies: [policy],
          slot: div(now_us, interval_us)
        )
      end
    end
  end

  defp due?(issue, policy, deliveries, now_us) do
    last =
      Map.get(deliveries, {issue["alert_issue_id"], policy.name}) ||
        attention_us(issue["attention_at"])

    now_us - last >= policy.repeat_interval_seconds * 1_000_000
  end

  defp attention_us(at) when is_binary(at) do
    case DateTime.from_iso8601(at) do
      {:ok, at, _offset} -> DateTime.to_unix(at, :microsecond)
      _ -> System.os_time(:microsecond)
    end
  end

  defp attention_us(_at), do: System.os_time(:microsecond)
end
