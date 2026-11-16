defmodule Logflare.AlertIssuesTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues
  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Sources

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)

    {:ok, alert} =
      Alerting.create_alert_query(user, %{
        name: "Edge function errors",
        cron: "*/5 * * * *",
        query: "select 1",
        language: :bq_sql,
        rule_key: "logflare.edge-fn-5xx",
        severity: :major,
        category: :performance
      })

    test_pid = self()

    stub(Backends, :ingest_logs, fn events, source ->
      send(test_pid, {:ingested, source, events})
      {:ok, length(events)}
    end)

    %{user: user, alert: alert}
  end

  defp signal(attrs \\ %{}) do
    Map.merge(
      %{
        fingerprint: "fp-1",
        status: :firing,
        identifiers: %{"project" => "default", "function_id" => "stripe-webhook"},
        observed_at: ~U[2026-11-16 12:00:00.000000Z]
      },
      attrs
    )
  end

  defp later(seconds), do: DateTime.add(~U[2026-11-16 12:00:00.000000Z], seconds, :second)

  defp assert_ingested do
    assert_receive {:ingested, source, [event]}
    {source, event}
  end

  test "first firing signal opens an alert issue in system.alert_issues", %{
    alert: alert,
    user: user
  } do
    assert {:ok, %{transition: :opened, snapshot: snapshot}} =
             AlertIssues.apply_signal(alert, signal())

    {source, event} = assert_ingested()
    assert source.user_id == user.id
    assert source.system_source_type == :alert_issues
    assert event == snapshot

    assert %{
             "status" => "open",
             "change" => "opened",
             "rule_key" => "logflare.edge-fn-5xx",
             "alert_token" => _,
             "fingerprint" => "fp-1",
             "severity" => "major",
             "category" => "performance",
             "title" => "Edge function errors",
             "signal_count" => 1,
             "identifiers" => %{"project" => "default", "function_id" => "stripe-webhook"},
             "actor" => "system",
             "event_message" => "Edge function errors opened"
           } = snapshot

    assert is_binary(snapshot["alert_issue_id"])
    assert snapshot["first_seen_at"] == snapshot["last_seen_at"]
    assert snapshot["attention_at"] == snapshot["first_seen_at"]
  end

  test "repeat firing signals roll up into the same alert issue", %{alert: alert} do
    {:ok, %{snapshot: opened}} = AlertIssues.apply_signal(alert, signal())
    _ = assert_ingested()

    assert {:ok, %{transition: :signal, snapshot: rolled}} =
             AlertIssues.apply_signal(alert, signal(%{observed_at: later(300)}))

    _ = assert_ingested()

    assert rolled["alert_issue_id"] == opened["alert_issue_id"]
    assert rolled["signal_count"] == 2
    assert rolled["version"] > opened["version"]
    assert rolled["first_seen_at"] == opened["first_seen_at"]
    assert rolled["attention_at"] == opened["attention_at"]
    assert rolled["last_seen_at"] > opened["last_seen_at"]
    assert rolled["change"] == "signal"
  end

  test "a resolved signal resolves the open alert issue", %{alert: alert} do
    {:ok, %{snapshot: opened}} = AlertIssues.apply_signal(alert, signal())
    _ = assert_ingested()

    assert {:ok, %{transition: :resolved, snapshot: resolved}} =
             AlertIssues.apply_signal(alert, signal(%{status: :resolved, observed_at: later(60)}))

    _ = assert_ingested()
    assert resolved["alert_issue_id"] == opened["alert_issue_id"]
    assert resolved["status"] == "resolved"
    assert resolved["change"] == "resolved"
  end

  test "a resolved signal without an active alert issue writes nothing", %{alert: alert} do
    assert {:ok, %{transition: :noop}} =
             AlertIssues.apply_signal(alert, signal(%{status: :resolved}))

    refute_receive {:ingested, _, _}
  end

  test "firing again within 24h of resolving reopens the same alert issue", %{alert: alert} do
    {:ok, %{snapshot: opened}} = AlertIssues.apply_signal(alert, signal())

    {:ok, _} =
      AlertIssues.apply_signal(alert, signal(%{status: :resolved, observed_at: later(60)}))

    assert {:ok, %{transition: :reopened, snapshot: reopened}} =
             AlertIssues.apply_signal(alert, signal(%{observed_at: later(3_600)}))

    assert reopened["alert_issue_id"] == opened["alert_issue_id"]
    assert reopened["status"] == "open"
    assert reopened["attention_at"] > opened["attention_at"]
  end

  test "firing more than 24h after resolving opens a new alert issue", %{alert: alert} do
    {:ok, %{snapshot: opened}} = AlertIssues.apply_signal(alert, signal())

    {:ok, _} =
      AlertIssues.apply_signal(alert, signal(%{status: :resolved, observed_at: later(60)}))

    assert {:ok, %{transition: :opened, snapshot: fresh}} =
             AlertIssues.apply_signal(alert, signal(%{observed_at: later(90_000)}))

    refute fresh["alert_issue_id"] == opened["alert_issue_id"]
    assert fresh["signal_count"] == 1
  end

  test "different fingerprints and users get separate alert issues", %{alert: alert} do
    {:ok, %{snapshot: a}} = AlertIssues.apply_signal(alert, signal())
    {:ok, %{snapshot: b}} = AlertIssues.apply_signal(alert, signal(%{fingerprint: "fp-2"}))
    refute a["alert_issue_id"] == b["alert_issue_id"]

    other_user = insert(:user)

    {:ok, other_alert} =
      Alerting.create_alert_query(
        other_user,
        Map.from_struct(alert) |> Map.take([:name, :cron, :query, :language, :rule_key])
      )

    assert {:ok, %{transition: :opened, snapshot: c}} =
             AlertIssues.apply_signal(other_alert, signal())

    refute c["alert_issue_id"] == a["alert_issue_id"]
  end

  test "creates the alert issue system sources on first use", %{alert: alert, user: user} do
    assert Sources.list_system_sources_by_user(user.id) == []
    {:ok, _} = AlertIssues.apply_signal(alert, signal())

    types = user.id |> Sources.list_system_sources_by_user() |> Enum.map(& &1.system_source_type)
    assert :alert_issues in types
  end

  test "signal title and description override the alert defaults", %{alert: alert} do
    {:ok, %{snapshot: snapshot}} =
      AlertIssues.apply_signal(
        alert,
        signal(%{title: "stripe-webhook 5xx", description: "18% of requests failed"})
      )

    assert snapshot["title"] == "stripe-webhook 5xx"
    assert snapshot["description"] == "18% of requests failed"
  end

  describe "process_run/2" do
    setup %{alert: alert} do
      {:ok, alert} =
        Alerting.update_alert_query(alert, %{group_by: ["function_id"], resolve_after_runs: 2})

      %{alert: alert}
    end

    defp transitions(results), do: results |> Enum.map(& &1.transition) |> Enum.sort()

    test "opens one alert issue per group", %{alert: alert} do
      rows = [%{"function_id" => "f1"}, %{"function_id" => "f2"}]
      assert {:ok, results} = AlertIssues.process_run(alert, rows)
      assert transitions(results) == [:opened, :opened]
    end

    test "resolves after resolve_after_runs clean runs", %{alert: alert} do
      {:ok, _} = AlertIssues.process_run(alert, [%{"function_id" => "f1"}])

      assert {:ok, []} = AlertIssues.process_run(alert, [])
      assert {:ok, [%{transition: :resolved}]} = AlertIssues.process_run(alert, [])
      assert {:ok, []} = AlertIssues.process_run(alert, [])
    end

    test "a group missing from a run counts a clean run while others keep firing", %{
      alert: alert
    } do
      {:ok, _} =
        AlertIssues.process_run(alert, [%{"function_id" => "f1"}, %{"function_id" => "f2"}])

      {:ok, first} = AlertIssues.process_run(alert, [%{"function_id" => "f2"}])
      assert transitions(first) == [:signal]

      {:ok, second} = AlertIssues.process_run(alert, [%{"function_id" => "f2"}])
      assert transitions(second) == [:resolved, :signal]
    end

    test "firing again resets the clean run count", %{alert: alert} do
      {:ok, _} = AlertIssues.process_run(alert, [%{"function_id" => "f1"}])
      {:ok, []} = AlertIssues.process_run(alert, [])
      {:ok, [%{transition: :signal}]} = AlertIssues.process_run(alert, [%{"function_id" => "f1"}])
      assert {:ok, []} = AlertIssues.process_run(alert, [])
    end
  end
end
