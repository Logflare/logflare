defmodule Logflare.AlertIssues.Notifications do
  @moduledoc """
  Decides which alert issue changes are sent to the control plane. Each matching
  enabled notification policy gets one `NotificationWorker` job per alert issue,
  policy, event and version (repeats: per interval slot).

  Suppression: silenced alert issues send nothing; an active snooze suppresses all but
  resolution; acknowledged alert issues get no repeats.
  """

  alias Logflare.AlertIssues.NotificationPolicies
  alias Logflare.AlertIssues.NotificationPolicy
  alias Logflare.AlertIssues.NotificationWorker
  alias Logflare.AlertIssues.Notifier
  alias Logflare.AlertIssues.Silences

  @issue_fields ~w(alert_issue_id title description status severity category rule_key
    identifiers signal_count first_seen_at last_seen_at version)

  @type event :: String.t()
  @type opt :: {:slot, non_neg_integer()} | {:policies, [NotificationPolicy.t()]}

  @doc """
  Enqueues notification events for an alert issue snapshot. `event` is one of
  #{inspect(NotificationPolicy.events())}; repeats need `slot: n` (the interval number).
  """
  @spec notify(pos_integer(), event(), map(), [opt()]) :: :ok
  def notify(user_id, event, snapshot, opts \\ []) do
    if Notifier.enabled?() and not suppressed?(user_id, event, snapshot) do
      opts
      |> Keyword.get_lazy(:policies, fn -> NotificationPolicies.list_enabled(user_id) end)
      |> Enum.filter(&NotificationPolicy.matches?(&1, event, snapshot))
      |> Enum.each(&enqueue(user_id, event, snapshot, &1, opts[:slot]))
    end

    :ok
  end

  @doc """
  Whether a change of an alert issue is suppressed by a silence, snooze or acknowledgement.
  """
  @spec suppressed?(pos_integer(), event(), map()) :: boolean()
  def suppressed?(user_id, event, snapshot) do
    Silences.silenced?(Silences.list(user_id), snapshot["rule_key"], snapshot["identifiers"]) or
      (event != "resolved" and snoozed?(snapshot)) or
      (event == "repeat" and snapshot["status"] == "acknowledged")
  end

  defp snoozed?(%{"status" => "snoozed", "snoozed_until" => until}) when is_binary(until) do
    case DateTime.from_iso8601(until) do
      {:ok, until, _offset} -> DateTime.after?(until, DateTime.utc_now())
      _ -> true
    end
  end

  defp snoozed?(%{"status" => "snoozed"}), do: true
  defp snoozed?(_snapshot), do: false

  defp enqueue(user_id, event, snapshot, policy, slot) do
    body = %{
      "event_id" => Ecto.UUID.generate(),
      "type" => "alert_issue.#{event}",
      "occurred_at" => DateTime.to_iso8601(DateTime.utc_now()),
      "policy" => %{"name" => policy.name, "receivers" => policy.receivers},
      "alert_issue" => Map.new(@issue_fields, &{&1, Map.get(snapshot, &1)})
    }

    dedupe_key =
      Enum.join(
        [snapshot["alert_issue_id"], policy.id, event, snapshot["version"], slot],
        ":"
      )

    %{"user_id" => user_id, "event" => body, "dedupe_key" => dedupe_key}
    |> NotificationWorker.new()
    |> Oban.insert!()
  end
end
