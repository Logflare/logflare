defmodule Logflare.AlertIssues do
  @moduledoc """
  Turns alert signals into versioned alert issue snapshots.

  Every change appends a full snapshot event to the user's `system.alert_issues`
  source. The current state of an alert issue is its highest `version`.
  """

  alias Logflare.AlertIssues.ActiveCache
  alias Logflare.AlertIssues.Queries
  alias Logflare.AlertIssues.Signals
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends
  alias Logflare.Sources
  alias Logflare.User

  @reopen_window_us 24 * 60 * 60 * 1_000_000
  @storage_fields ["id"]
  @max_comment_length 5000

  @type signal :: %{
          required(:fingerprint) => String.t(),
          required(:status) => :firing | :resolved,
          optional(:identifiers) => map(),
          optional(:title) => String.t(),
          optional(:description) => String.t(),
          optional(:metadata) => map(),
          optional(:severity) => :critical | :major | :warning | :info,
          optional(:category) => :security | :performance | :health,
          optional(:observed_at) => DateTime.t()
        }

  @type transition :: :opened | :signal | :resolved | :reopened | :noop
  @type action :: :acknowledge | :snooze | :resolve | :reopen

  @spec apply_signal(AlertQuery.t(), signal()) ::
          {:ok, %{transition: transition(), snapshot: map() | nil}} | {:error, term()}
  def apply_signal(%AlertQuery{} = alert, %{fingerprint: fingerprint, status: status} = signal)
      when status in [:firing, :resolved] do
    observed_us =
      signal
      |> Map.get(:observed_at, DateTime.utc_now())
      |> DateTime.to_unix(:microsecond)

    with {:ok, current} <- current_snapshot(alert.user_id, fingerprint) do
      apply_transition(alert, signal, current, observed_us)
    end
  end

  defp apply_transition(alert, signal, current, observed_us) do
    case next_snapshot(alert, signal, current, observed_us) do
      :noop ->
        {:ok, %{transition: :noop, snapshot: nil}}

      {transition, snapshot} ->
        snapshot =
          Map.put(
            snapshot,
            "silenced",
            Silences.silenced?(
              Silences.list(alert.user_id),
              alert.rule_key,
              snapshot["identifiers"]
            )
          )

        with {:ok, _count} <- write(alert.user_id, snapshot) do
          ActiveCache.put(alert.user_id, signal.fingerprint, snapshot)
          {:ok, %{transition: transition, snapshot: snapshot}}
        end
    end
  end

  defp current_snapshot(user_id, fingerprint) do
    case ActiveCache.get(user_id, fingerprint) do
      nil -> load_snapshot(user_id, fingerprint)
      snapshot -> {:ok, snapshot}
    end
  end

  defp load_snapshot(user_id, fingerprint) do
    with {:ok, %{} = stored} <- Queries.latest_by_fingerprint(user_id, fingerprint) do
      snapshot = Map.drop(stored, @storage_fields)
      ActiveCache.put(user_id, fingerprint, snapshot)
      {:ok, snapshot}
    end
  end

  @doc """
  Applies a user action to an alert issue found within `opts[:filters]` and appends
  the new version. Options: `:filters` (required), `:actor`, `:until` (snooze).
  """
  @spec act(User.t(), String.t(), action(), keyword()) :: {:ok, map()} | {:error, term()}
  def act(%User{} = user, alert_issue_id, action, opts) do
    with {:ok, stored} <- Queries.get(user, alert_issue_id, Keyword.take(opts, [:filters])),
         current = Map.drop(stored, @storage_fields),
         version = max(System.os_time(:microsecond), current["version"] + 1),
         {:ok, attrs, change} <-
           action_transition(action, current, Keyword.put(opts, :version, version)) do
      snapshot =
        current
        |> Map.merge(attrs)
        |> Map.merge(%{
          "version" => version,
          "timestamp" => version,
          "change" => change,
          "actor" => Keyword.get(opts, :actor, "api"),
          "event_message" => "#{current["title"]} #{change}"
        })

      with {:ok, _count} <- write(user.id, snapshot) do
        ActiveCache.put(user.id, snapshot["fingerprint"], snapshot)
        {:ok, snapshot}
      end
    end
  end

  @doc """
  Marks alert issues read or unread for a reader by appending one event per alert
  issue to `system.alert_issue_reads`. Every id must exist within `opts[:filters]`.
  """
  @spec mark_read(User.t(), [String.t()], String.t(), boolean(), keyword()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  def mark_read(%User{} = user, alert_issue_ids, reader, read, opts)
      when is_list(alert_issue_ids) and is_binary(reader) and is_boolean(read) do
    filters = Keyword.take(opts, [:filters])

    if Enum.all?(alert_issue_ids, &match?({:ok, _}, Queries.get(user, &1, filters))) do
      version = System.os_time(:microsecond)

      events =
        Enum.map(alert_issue_ids, fn id ->
          %{
            "alert_issue_id" => id,
            "reader" => reader,
            "read" => read,
            "version" => version,
            "timestamp" => version,
            "event_message" => if(read, do: "read", else: "unread")
          }
        end)

      Backends.ingest_logs(events, system_source(user.id, :alert_issue_reads))
    else
      {:error, :not_found}
    end
  end

  @doc """
  Appends a comment to an alert issue visible within `opts[:filters]` in
  `system.alert_issue_comments`. Options: `:filters` (required), `:author`.
  """
  @spec comment(User.t(), String.t(), String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def comment(%User{} = user, alert_issue_id, body, opts) when is_binary(body) do
    with :ok <- validate_comment_body(body),
         {:ok, _issue} <- Queries.get(user, alert_issue_id, Keyword.take(opts, [:filters])) do
      version = System.os_time(:microsecond)

      comment = %{
        "comment_id" => Ecto.UUID.generate(),
        "alert_issue_id" => alert_issue_id,
        "author" => Keyword.get(opts, :author, "api"),
        "body" => String.trim(body),
        "created_at" => iso(version)
      }

      event =
        Map.merge(comment, %{
          "version" => version,
          "timestamp" => version,
          "event_message" => comment["body"]
        })

      with {:ok, _count} <-
             Backends.ingest_logs([event], system_source(user.id, :alert_issue_comments)) do
        {:ok, comment}
      end
    end
  end

  defp validate_comment_body(body) do
    trimmed = String.trim(body)

    if trimmed != "" and String.length(trimmed) <= @max_comment_length,
      do: :ok,
      else: {:error, :invalid_body}
  end

  defp action_transition(:acknowledge, %{"status" => "open"}, _opts),
    do: {:ok, %{"status" => "acknowledged"}, "acknowledged"}

  defp action_transition(:snooze, current, opts) do
    case {Keyword.get(opts, :until), current["status"]} do
      {nil, _status} ->
        {:error, :until_required}

      {%DateTime{} = until, status} when status in ["open", "acknowledged"] ->
        {:ok, %{"status" => "snoozed", "snoozed_until" => DateTime.to_iso8601(until)}, "snoozed"}

      _ ->
        {:error, :invalid_transition}
    end
  end

  defp action_transition(:resolve, %{"status" => status}, _opts) when status != "resolved",
    do: {:ok, %{"status" => "resolved"}, "resolved"}

  defp action_transition(:reopen, %{"status" => "resolved"}, opts) do
    {:ok, %{"status" => "open", "attention_at" => iso(Keyword.fetch!(opts, :version))},
     "reopened"}
  end

  defp action_transition(_action, _current, _opts), do: {:error, :invalid_transition}

  @doc """
  Applies one scheduled run of an alert: every result group fires, and active alert
  issues missing from the run are resolved after `resolve_after_runs` clean runs.
  """
  @spec process_run(AlertQuery.t(), [map()]) ::
          {:ok, [%{transition: transition(), snapshot: map() | nil}]} | {:error, term()}
  def process_run(%AlertQuery{} = alert, rows) when is_list(rows) do
    signals = Signals.from_rows(alert, rows)
    firing = MapSet.new(signals, & &1.fingerprint)

    resolutions =
      alert.user_id
      |> ActiveCache.active_fingerprints(to_string(alert.token))
      |> Enum.reject(&MapSet.member?(firing, &1))
      |> Enum.filter(&clean_run_reached?(alert, &1))
      |> Enum.map(&%{fingerprint: &1, status: :resolved})

    Enum.each(firing, &ActiveCache.reset_clean_runs(alert.user_id, &1))

    (signals ++ resolutions)
    |> Enum.reduce_while({:ok, []}, fn signal, {:ok, acc} ->
      case apply_signal(alert, signal) do
        {:ok, %{transition: :noop}} -> {:cont, {:ok, acc}}
        {:ok, result} -> {:cont, {:ok, [result | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
  end

  defp clean_run_reached?(alert, fingerprint) do
    if ActiveCache.increment_clean_runs(alert.user_id, fingerprint) >= alert.resolve_after_runs do
      ActiveCache.reset_clean_runs(alert.user_id, fingerprint)
      true
    else
      false
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
    at = iso(us)

    attrs = %{
      "status" => "open",
      "attention_at" => at,
      "signal_count" => current["signal_count"] + 1,
      "last_seen_at" => at
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
    at = iso(us)

    %{
      "alert_issue_id" => alert_issue_id(alert.user_id, signal.fingerprint, us),
      "version" => us,
      "timestamp" => us,
      "rule_key" => alert.rule_key,
      "alert_token" => to_string(alert.token),
      "fingerprint" => signal.fingerprint,
      "status" => "open",
      "change" => "opened",
      "severity" => Atom.to_string(Map.get(signal, :severity) || alert.severity),
      "category" => Atom.to_string(Map.get(signal, :category) || alert.category),
      "title" => title,
      "description" => Map.get(signal, :description) || alert.description,
      "identifiers" => Map.get(signal, :identifiers, %{}),
      "metadata" => Map.get(signal, :metadata, %{}),
      "notebook" => notebook(alert),
      "signal_count" => 1,
      "first_seen_at" => at,
      "last_seen_at" => at,
      "attention_at" => at,
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
      "severity" => enum_string(Map.get(signal, :severity)) || current["severity"],
      "category" => enum_string(Map.get(signal, :category)) || current["category"],
      "identifiers" => Map.get(signal, :identifiers, current["identifiers"]),
      "metadata" => Map.get(signal, :metadata, current["metadata"]),
      "notebook" => notebook(alert),
      "actor" => "system",
      "event_message" => "#{title} #{change}"
    })
  end

  defp write(user_id, snapshot) do
    Backends.ingest_logs([snapshot], system_source(user_id, :alert_issues))
  end

  defp system_source(user_id, type) do
    case Sources.Cache.get_by(user_id: user_id, system_source_type: type) do
      nil ->
        user_id
        |> Sources.create_alert_issue_system_sources()
        |> Enum.find(&(&1.system_source_type == type))

      source ->
        source
    end
  end

  defp alert_issue_id(user_id, fingerprint, opened_us) do
    :crypto.hash(:sha256, "#{user_id}:#{fingerprint}:#{opened_us}")
    |> Base.encode16(case: :lower)
    |> binary_part(0, 32)
  end

  defp notebook(%AlertQuery{signal_mapping: %{"notebook" => notebook}}) when is_list(notebook),
    do: notebook

  defp notebook(_alert), do: []

  defp enum_string(nil), do: nil
  defp enum_string(value), do: Atom.to_string(value)

  defp iso(us), do: us |> DateTime.from_unix!(:microsecond) |> DateTime.to_iso8601()
end
