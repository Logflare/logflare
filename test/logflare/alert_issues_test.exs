defmodule Logflare.AlertIssuesTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.Queries
  alias Logflare.AlertIssues.Silences
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

  test "copies the rule's notebook templates onto every snapshot", %{alert: alert} do
    {:ok, %{snapshot: snapshot}} = AlertIssues.apply_signal(alert, signal())
    assert snapshot["notebook"] == []

    notebook = [%{"title" => "Errors", "source" => "logs", "sql" => "select '{{function_id}}'"}]
    alert = %{alert | signal_mapping: %{"notebook" => notebook}}

    {:ok, %{snapshot: snapshot}} =
      AlertIssues.apply_signal(alert, signal(%{observed_at: later(60)}))

    assert snapshot["notebook"] == notebook
  end

  test "signal severity and category override the alert defaults", %{alert: alert} do
    {:ok, %{snapshot: snapshot}} =
      AlertIssues.apply_signal(alert, signal(%{severity: :critical, category: :security}))

    assert snapshot["severity"] == "critical"
    assert snapshot["category"] == "security"

    {:ok, %{snapshot: snapshot}} =
      AlertIssues.apply_signal(alert, signal(%{severity: :warning, observed_at: later(60)}))

    assert snapshot["severity"] == "warning"
    assert snapshot["category"] == "security"
  end

  test "stamps silenced on alert issues matched by a silence", %{alert: alert, user: user} do
    {:ok, %{snapshot: snapshot}} = AlertIssues.apply_signal(alert, signal())
    assert snapshot["silenced"] == false

    {:ok, _} =
      Silences.create(user, %{
        "rule_key" => alert.rule_key,
        "matchers" => %{"project" => "default"}
      })

    {:ok, %{snapshot: snapshot}} =
      AlertIssues.apply_signal(alert, signal(%{observed_at: later(60)}))

    assert snapshot["silenced"] == true
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

  describe "act/4" do
    setup %{alert: alert, user: user} do
      {:ok, %{snapshot: opened}} = AlertIssues.apply_signal(alert, signal())
      _ = assert_ingested()

      stub(Queries, :get, fn %{id: id}, alert_issue_id, opts ->
        assert id == user.id
        assert opts[:filters] == %{"project" => "default"}

        if alert_issue_id == opened["alert_issue_id"],
          do: {:ok, AlertIssues.ActiveCache.get(user.id, "fp-1")},
          else: {:error, :not_found}
      end)

      %{opened: opened}
    end

    defp act(user, id, action, extra \\ []) do
      AlertIssues.act(
        user,
        id,
        action,
        Keyword.merge([filters: %{"project" => "default"}, actor: "dev@example.com"], extra)
      )
    end

    test "acknowledges an open alert issue", %{user: user, opened: opened} do
      assert {:ok, acked} = act(user, opened["alert_issue_id"], :acknowledge)
      {_source, event} = assert_ingested()

      assert event == acked
      assert acked["status"] == "acknowledged"
      assert acked["change"] == "acknowledged"
      assert acked["actor"] == "dev@example.com"
      assert acked["version"] > opened["version"]
      assert acked["attention_at"] == opened["attention_at"]
    end

    test "appends a new event rather than reusing the stored event id", %{
      user: user,
      opened: opened
    } do
      stored = Map.merge(opened, %{"id" => "stored-event-id", "event_message" => "stored"})
      stub(Queries, :get, fn _user, _id, _opts -> {:ok, stored} end)

      assert {:ok, acked} = act(user, opened["alert_issue_id"], :acknowledge)
      {_source, event} = assert_ingested()

      refute Map.has_key?(event, "id")
      refute Map.has_key?(acked, "id")
      assert event["event_message"] == "Edge function errors acknowledged"
    end

    test "rejects invalid transitions", %{user: user, opened: opened} do
      {:ok, _} = act(user, opened["alert_issue_id"], :acknowledge)
      assert {:error, :invalid_transition} = act(user, opened["alert_issue_id"], :acknowledge)
      assert {:error, :invalid_transition} = act(user, opened["alert_issue_id"], :reopen)
    end

    test "snoozes until a time", %{user: user, opened: opened} do
      until = ~U[2026-11-16 13:00:00Z]

      assert {:ok, snoozed} = act(user, opened["alert_issue_id"], :snooze, until: until)
      assert snoozed["status"] == "snoozed"
      assert snoozed["snoozed_until"] == DateTime.to_iso8601(until)

      assert {:error, :until_required} = act(user, opened["alert_issue_id"], :snooze)
    end

    test "resolves and reopens", %{user: user, opened: opened} do
      assert {:ok, resolved} = act(user, opened["alert_issue_id"], :resolve)
      assert resolved["status"] == "resolved"

      assert {:ok, reopened} = act(user, opened["alert_issue_id"], :reopen)
      assert reopened["status"] == "open"
      assert reopened["change"] == "reopened"
      assert reopened["attention_at"] > opened["attention_at"]
    end

    test "a manually resolved alert issue reopens on the next firing signal", %{
      user: user,
      alert: alert,
      opened: opened
    } do
      {:ok, _} = act(user, opened["alert_issue_id"], :resolve)

      assert {:ok, %{transition: :reopened}} =
               AlertIssues.apply_signal(alert, signal(%{observed_at: later(60)}))
    end

    test "returns not found outside the filters", %{user: user} do
      assert {:error, :not_found} = act(user, "missing", :acknowledge)
    end
  end

  describe "mark_read/5" do
    setup %{user: user} do
      stub(Queries, :get, fn _user, id, opts ->
        assert opts[:filters] == %{"project" => "default"}
        if id in ["a", "b"], do: {:ok, %{"alert_issue_id" => id}}, else: {:error, :not_found}
      end)

      %{user: user}
    end

    test "appends one read event per alert issue to system.alert_issue_reads", %{user: user} do
      assert {:ok, 2} =
               AlertIssues.mark_read(user, ["a", "b"], "user_1", true,
                 filters: %{"project" => "default"}
               )

      assert_receive {:ingested, source, events}
      assert source.system_source_type == :alert_issue_reads

      assert [
               %{"alert_issue_id" => "a", "reader" => "user_1", "read" => true},
               %{"alert_issue_id" => "b"}
             ] =
               Enum.sort_by(events, & &1["alert_issue_id"])

      assert Enum.all?(events, &is_integer(&1["version"]))
    end

    test "marks unread", %{user: user} do
      assert {:ok, 1} =
               AlertIssues.mark_read(user, ["a"], "user_1", false,
                 filters: %{"project" => "default"}
               )

      assert_receive {:ingested, _source, [%{"read" => false}]}
    end

    test "rejects alert issues outside the filters", %{user: user} do
      assert {:error, :not_found} =
               AlertIssues.mark_read(user, ["a", "zzz"], "user_1", true,
                 filters: %{"project" => "default"}
               )

      refute_receive {:ingested, _, _}
    end
  end

  describe "comment/4" do
    setup %{user: user} do
      stub(Queries, :get, fn _user, id, opts ->
        assert opts[:filters] == %{"project" => "default"}
        if id == "a", do: {:ok, %{"alert_issue_id" => id}}, else: {:error, :not_found}
      end)

      %{user: user}
    end

    test "appends a comment event to system.alert_issue_comments", %{user: user} do
      assert {:ok, comment} =
               AlertIssues.comment(user, "a", "Looking into it",
                 author: "ops@example.com",
                 filters: %{"project" => "default"}
               )

      assert %{
               "alert_issue_id" => "a",
               "author" => "ops@example.com",
               "body" => "Looking into it",
               "comment_id" => comment_id,
               "created_at" => created_at
             } = comment

      assert is_binary(comment_id) and is_binary(created_at)

      assert_receive {:ingested, source, [event]}
      assert source.system_source_type == :alert_issue_comments
      assert event["comment_id"] == comment_id
      assert is_integer(event["version"])
    end

    test "rejects blank and oversized comments", %{user: user} do
      opts = [author: "a", filters: %{"project" => "default"}]
      assert {:error, :invalid_body} = AlertIssues.comment(user, "a", "   ", opts)

      assert {:error, :invalid_body} =
               AlertIssues.comment(user, "a", String.duplicate("x", 5001), opts)

      refute_receive {:ingested, _, _}
    end

    test "rejects alert issues outside the filters", %{user: user} do
      assert {:error, :not_found} =
               AlertIssues.comment(user, "zzz", "hi",
                 author: "a",
                 filters: %{"project" => "default"}
               )

      refute_receive {:ingested, _, _}
    end
  end
end
