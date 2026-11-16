defmodule Logflare.AlertIssues do
  @moduledoc """
  Turns alert signals into versioned alert issue snapshots.

  Every change appends a full snapshot event to the user's `system.alert_issues`
  source. The current state of an alert issue is its highest `version`.
  """

  alias Logflare.AlertIssues.ActiveCache
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends
  alias Logflare.Sources

  @reopen_window_us 24 * 60 * 60 * 1_000_000

  @type signal :: %{
          required(:fingerprint) => String.t(),
          required(:status) => :firing | :resolved,
          optional(:identifiers) => map(),
          optional(:title) => String.t(),
          optional(:description) => String.t(),
          optional(:metadata) => map(),
          optional(:observed_at) => DateTime.t()
        }

  @type transition :: :opened | :signal | :resolved | :reopened | :noop

  @spec apply_signal(AlertQuery.t(), signal()) ::
          {:ok, %{transition: transition(), snapshot: map() | nil}} | {:error, term()}
  def apply_signal(%AlertQuery{} = alert, %{fingerprint: fingerprint, status: status} = signal)
      when status in [:firing, :resolved] do
    observed_us =
      signal
      |> Map.get(:observed_at, DateTime.utc_now())
      |> DateTime.to_unix(:microsecond)

    current = ActiveCache.get(alert.user_id, fingerprint)

    case next_snapshot(alert, signal, current, observed_us) do
      :noop ->
        {:ok, %{transition: :noop, snapshot: nil}}

      {transition, snapshot} ->
        with {:ok, _count} <- write(alert.user_id, snapshot) do
          ActiveCache.put(alert.user_id, fingerprint, snapshot)
          {:ok, %{transition: transition, snapshot: snapshot}}
        end
    end
  end

  defp next_snapshot(_alert, %{status: :resolved}, nil, _us), do: :noop
  defp next_snapshot(_alert, %{status: :resolved}, %{"status" => "resolved"}, _us), do: :noop

  defp next_snapshot(alert, %{status: :resolved} = signal, current, us) do
    {:resolved, advance(alert, signal, current, us, %{"status" => "resolved"}, "resolved")}
  end

  defp next_snapshot(alert, signal, nil, us), do: {:opened, new_snapshot(alert, signal, us)}

  defp next_snapshot(alert, signal, %{"status" => "resolved", "version" => version} = current, us)
       when us - version <= @reopen_window_us do
    attrs = %{
      "status" => "open",
      "attention_at" => iso(us),
      "signal_count" => current["signal_count"] + 1,
      "last_seen_at" => iso(us)
    }

    {:reopened, advance(alert, signal, current, us, attrs, "reopened")}
  end

  defp next_snapshot(alert, signal, %{"status" => "resolved"}, us),
    do: {:opened, new_snapshot(alert, signal, us)}

  defp next_snapshot(alert, signal, current, us) do
    attrs = %{"signal_count" => current["signal_count"] + 1, "last_seen_at" => iso(us)}
    {:signal, advance(alert, signal, current, us, attrs, "signal")}
  end

  defp new_snapshot(alert, signal, us) do
    title = Map.get(signal, :title) || alert.name

    %{
      "alert_issue_id" => alert_issue_id(alert.user_id, signal.fingerprint, us),
      "version" => us,
      "timestamp" => us,
      "rule_key" => alert.rule_key,
      "alert_token" => to_string(alert.token),
      "fingerprint" => signal.fingerprint,
      "status" => "open",
      "change" => "opened",
      "severity" => Atom.to_string(alert.severity),
      "category" => Atom.to_string(alert.category),
      "title" => title,
      "description" => Map.get(signal, :description) || alert.description,
      "identifiers" => Map.get(signal, :identifiers, %{}),
      "metadata" => Map.get(signal, :metadata, %{}),
      "signal_count" => 1,
      "first_seen_at" => iso(us),
      "last_seen_at" => iso(us),
      "attention_at" => iso(us),
      "actor" => "system",
      "event_message" => "#{title} opened"
    }
  end

  defp advance(alert, signal, current, us, attrs, change) do
    version = max(us, current["version"] + 1)
    title = Map.get(signal, :title) || current["title"] || alert.name

    current
    |> Map.merge(attrs)
    |> Map.merge(%{
      "version" => version,
      "timestamp" => version,
      "change" => change,
      "title" => title,
      "description" => Map.get(signal, :description) || current["description"],
      "identifiers" => Map.get(signal, :identifiers, current["identifiers"]),
      "metadata" => Map.get(signal, :metadata, current["metadata"]),
      "actor" => "system",
      "event_message" => "#{title} #{change}"
    })
  end

  defp write(user_id, snapshot) do
    Backends.ingest_logs([snapshot], alert_issues_source(user_id))
  end

  defp alert_issues_source(user_id) do
    case Sources.Cache.get_by(user_id: user_id, system_source_type: :alert_issues) do
      nil ->
        user_id
        |> Sources.create_alert_issue_system_sources()
        |> Enum.find(&(&1.system_source_type == :alert_issues))

      source ->
        source
    end
  end

  defp alert_issue_id(user_id, fingerprint, opened_us) do
    :crypto.hash(:sha256, "#{user_id}:#{fingerprint}:#{opened_us}")
    |> Base.encode16(case: :lower)
    |> binary_part(0, 32)
  end

  defp iso(us), do: us |> DateTime.from_unix!(:microsecond) |> DateTime.to_iso8601()
end
