defmodule Logflare.AlertIssues.NotificationsTest do
  use Logflare.DataCase, async: false
  use Oban.Testing, repo: Logflare.Repo

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.ActiveCache
  alias Logflare.AlertIssues.NotificationPolicies
  alias Logflare.AlertIssues.Notifications
  alias Logflare.AlertIssues.NotificationWorker
  alias Logflare.AlertIssues.Notifier
  alias Logflare.AlertIssues.Queries
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting
  alias Logflare.Backends

  @snapshot %{
    "alert_issue_id" => "a1",
    "version" => 1_700_000_000_000_000,
    "title" => "RLS Disabled in Public: public.orders",
    "description" => "Table public.orders is public, but RLS has not been enabled.",
    "status" => "open",
    "severity" => "critical",
    "category" => "security",
    "rule_key" => "supabase.advisor.rls_disabled_in_public",
    "identifiers" => %{"project" => "default", "table" => "orders"},
    "signal_count" => 1,
    "first_seen_at" => "2026-11-17T12:00:00Z",
    "last_seen_at" => "2026-11-17T12:00:00Z",
    "fingerprint" => "fp",
    "metadata" => %{"sample_rows" => []}
  }

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    previous = Application.get_env(:logflare, Notifier)
    Application.put_env(:logflare, Notifier, url: "http://control-plane.test/hook", secret: "s")
    on_exit(fn -> Application.put_env(:logflare, Notifier, previous) end)

    {:ok, policy} =
      NotificationPolicies.create(user, %{
        "name" => "advisors",
        "rule_keys" => ["supabase.advisor.*"],
        "events" => ["opened", "reopened", "repeat", "resolved"],
        "receivers" => ["ops-webhook"]
      })

    %{user: user, policy: policy}
  end

  defp enqueued_events, do: Enum.map(all_enqueued(worker: NotificationWorker), & &1.args)

  test "enqueues one event per matching policy with the control plane contract", %{user: user} do
    {:ok, _} =
      NotificationPolicies.create(user, %{
        "name" => "edge",
        "rule_keys" => ["supabase.edge-*"],
        "receivers" => ["x"]
      })

    assert :ok = Notifications.notify(user.id, "opened", @snapshot)

    assert [%{"user_id" => user_id, "event" => event}] = enqueued_events()
    assert user_id == user.id

    assert %{
             "event_id" => event_id,
             "type" => "alert_issue.opened",
             "occurred_at" => occurred_at,
             "policy" => %{"name" => "advisors", "receivers" => ["ops-webhook"]},
             "alert_issue" => alert_issue
           } = event

    assert {:ok, _} = Ecto.UUID.cast(event_id)
    assert {:ok, _, _} = DateTime.from_iso8601(occurred_at)

    assert alert_issue ==
             Map.take(@snapshot, ~w(alert_issue_id title description status severity category
               rule_key identifiers signal_count first_seen_at last_seen_at version))

    assert Map.keys(event) |> Enum.sort() ==
             ~w(alert_issue event_id occurred_at policy type)
  end

  test "sends at most one event per alert issue, policy, type and version", %{user: user} do
    assert :ok = Notifications.notify(user.id, "opened", @snapshot)
    assert :ok = Notifications.notify(user.id, "opened", @snapshot)
    assert length(enqueued_events()) == 1

    assert :ok = Notifications.notify(user.id, "resolved", %{@snapshot | "version" => 2})
    assert length(enqueued_events()) == 2
  end

  test "does nothing when the notifier is not configured", %{user: user} do
    Application.put_env(:logflare, Notifier, url: nil, secret: nil)
    assert :ok = Notifications.notify(user.id, "opened", @snapshot)
    assert enqueued_events() == []
  end

  test "silenced alert issues are not sent", %{user: user} do
    {:ok, _} =
      Silences.create(user, %{
        "rule_key" => @snapshot["rule_key"],
        "matchers" => %{"project" => "default"}
      })

    assert :ok = Notifications.notify(user.id, "opened", @snapshot)
    assert enqueued_events() == []
  end

  test "an active snooze suppresses everything but resolution", %{user: user} do
    until = DateTime.utc_now() |> DateTime.add(3600) |> DateTime.to_iso8601()
    snoozed = %{@snapshot | "status" => "snoozed"} |> Map.put("snoozed_until", until)

    assert :ok = Notifications.notify(user.id, "repeat", snoozed, slot: 1)
    assert enqueued_events() == []

    expired = Map.put(snoozed, "snoozed_until", "2020-01-01T00:00:00Z")
    assert :ok = Notifications.notify(user.id, "repeat", expired, slot: 1)
    assert length(enqueued_events()) == 1
  end

  test "acknowledged alert issues get no repeats but still report resolution", %{user: user} do
    acked = %{@snapshot | "status" => "acknowledged"}

    assert :ok = Notifications.notify(user.id, "repeat", acked, slot: 1)
    assert enqueued_events() == []

    assert :ok = Notifications.notify(user.id, "resolved", %{acked | "status" => "resolved"})
    assert [%{"event" => %{"type" => "alert_issue.resolved"}}] = enqueued_events()
  end

  test "repeats are deduplicated per interval slot", %{user: user} do
    assert :ok = Notifications.notify(user.id, "repeat", @snapshot, slot: 7)
    assert :ok = Notifications.notify(user.id, "repeat", @snapshot, slot: 7)
    assert :ok = Notifications.notify(user.id, "repeat", @snapshot, slot: 8)
    assert length(enqueued_events()) == 2
  end

  describe "alert issue changes" do
    setup %{user: user} do
      {:ok, alert} =
        Alerting.create_alert_query(user, %{
          name: "RLS Disabled in Public",
          cron: "*/5 * * * *",
          query: "select 1",
          language: :bq_sql,
          rule_key: "supabase.advisor.rls_disabled_in_public",
          severity: :critical,
          category: :security
        })

      stub(Backends, :ingest_logs, fn events, _source -> {:ok, length(events)} end)
      %{alert: alert}
    end

    test "opening, resolving and reopening an alert issue notify", %{alert: alert} do
      signal = %{fingerprint: "fp-n", status: :firing, identifiers: %{"project" => "default"}}

      assert {:ok, %{transition: :opened}} = AlertIssues.apply_signal(alert, signal)
      assert {:ok, %{transition: :signal}} = AlertIssues.apply_signal(alert, signal)

      assert {:ok, %{transition: :resolved}} =
               AlertIssues.apply_signal(alert, %{signal | status: :resolved})

      assert {:ok, %{transition: :reopened}} = AlertIssues.apply_signal(alert, signal)

      assert enqueued_events() |> Enum.map(& &1["event"]["type"]) |> Enum.sort() == [
               "alert_issue.opened",
               "alert_issue.reopened",
               "alert_issue.resolved"
             ]
    end

    test "resolving and reopening by user action notify", %{alert: alert, user: user} do
      signal = %{fingerprint: "fp-a", status: :firing, identifiers: %{"project" => "default"}}
      {:ok, %{snapshot: opened}} = AlertIssues.apply_signal(alert, signal)
      stub(Queries, :get, fn _user, _id, _opts -> {:ok, ActiveCache.get(user.id, "fp-a")} end)
      opts = [filters: %{"project" => "default"}]

      assert {:ok, _} = AlertIssues.act(user, opened["alert_issue_id"], :acknowledge, opts)
      assert {:ok, _} = AlertIssues.act(user, opened["alert_issue_id"], :resolve, opts)
      assert {:ok, _} = AlertIssues.act(user, opened["alert_issue_id"], :reopen, opts)

      assert enqueued_events() |> Enum.map(& &1["event"]["type"]) |> Enum.sort() == [
               "alert_issue.opened",
               "alert_issue.reopened",
               "alert_issue.resolved"
             ]
    end
  end
end
