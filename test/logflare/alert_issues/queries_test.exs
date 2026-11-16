defmodule Logflare.AlertIssues.QueriesTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.Queries
  alias Logflare.AlertIssues.Silences
  alias Logflare.Alerting
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Sources

  setup do
    insert(:plan, name: "Free", limit_source_ttl: :timer.hours(24 * 90))
    user = insert(:user)
    sources = Sources.create_alert_issue_system_sources(user.id)
    source = Enum.find(sources, &(&1.system_source_type == :alert_issues))
    reads_source = Enum.find(sources, &(&1.system_source_type == :alert_issue_reads))
    comments_source = Enum.find(sources, &(&1.system_source_type == :alert_issue_comments))

    cfg = Application.get_env(:logflare, Logflare.Repo)
    url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

    backend =
      insert(:backend,
        type: :postgres,
        config: %{url: url},
        sources: [source, reads_source, comments_source],
        user: user
      )

    PostgresAdaptor.create_repo(backend)

    for s <- [source, reads_source, comments_source] do
      PostgresAdaptor.create_events_table({s, backend})
      on_exit(fn -> PostgresAdaptor.destroy_instance({s, backend}) end)
    end

    now = DateTime.utc_now()

    %{
      user: user,
      source: source,
      reads_source: reads_source,
      comments_source: comments_source,
      backend: backend,
      now: now
    }
  end

  defp insert_snapshot(%{source: source, backend: backend}, attrs, at) do
    us = DateTime.to_unix(at, :microsecond)

    body =
      Map.merge(
        %{
          "version" => us,
          "status" => "open",
          "title" => "t",
          "last_seen_at" => DateTime.to_iso8601(at),
          "identifiers" => %{"project" => "default"}
        },
        attrs
      )

    {:ok, _} =
      PostgresAdaptor.execute_query(
        backend,
        {"insert into #{PostgresAdaptor.table_name(source)} (id, body, event_message, timestamp) values ($1, $2, $3, $4) returning id",
         [Ecto.UUID.generate(), body, "snapshot", at]},
        []
      )

    body
  end

  defp ago(now, seconds), do: DateTime.add(now, -seconds, :second)

  defp insert_read(%{reads_source: source, backend: backend}, alert_issue_id, read, at) do
    body = %{
      "alert_issue_id" => alert_issue_id,
      "reader" => "user_1",
      "read" => read,
      "version" => DateTime.to_unix(at, :microsecond)
    }

    {:ok, _} =
      PostgresAdaptor.execute_query(
        backend,
        {"insert into #{PostgresAdaptor.table_name(source)} (id, body, event_message, timestamp) values ($1, $2, $3, $4) returning id",
         [Ecto.UUID.generate(), body, "read", at]},
        []
      )
  end

  defp unread_by_id(ctx) do
    {:ok, issues} =
      Queries.list(ctx.user, filters: %{"project" => "default"}, reader: "user_1")

    Map.new(issues, &{&1["alert_issue_id"], &1["unread"]})
  end

  test "returns the latest version of each alert issue matching the filters", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "a", "status" => "open"}, ago(ctx.now, 300))
    insert_snapshot(ctx, %{"alert_issue_id" => "a", "status" => "acknowledged"}, ago(ctx.now, 60))
    insert_snapshot(ctx, %{"alert_issue_id" => "b", "status" => "resolved"}, ago(ctx.now, 120))

    insert_snapshot(
      ctx,
      %{"alert_issue_id" => "c", "identifiers" => %{"project" => "other"}},
      ago(ctx.now, 30)
    )

    assert {:ok, issues} = Queries.list(ctx.user, filters: %{"project" => "default"})

    assert issues |> Enum.map(&{&1["alert_issue_id"], &1["status"]}) |> Enum.sort() == [
             {"a", "acknowledged"},
             {"b", "resolved"}
           ]
  end

  test "orders by last_seen_at, newest first", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "old"}, ago(ctx.now, 600))
    insert_snapshot(ctx, %{"alert_issue_id" => "new"}, ago(ctx.now, 10))

    assert {:ok, [%{"alert_issue_id" => "new"}, %{"alert_issue_id" => "old"}]} =
             Queries.list(ctx.user, filters: %{"project" => "default"})
  end

  test "filters by status", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "a", "status" => "open"}, ago(ctx.now, 60))
    insert_snapshot(ctx, %{"alert_issue_id" => "b", "status" => "resolved"}, ago(ctx.now, 60))

    assert {:ok, [%{"alert_issue_id" => "b"}]} =
             Queries.list(ctx.user,
               filters: %{"project" => "default"},
               statuses: ["resolved"]
             )
  end

  test "excludes alert issue versions older than since", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "stale"}, ago(ctx.now, 10 * 86_400))
    insert_snapshot(ctx, %{"alert_issue_id" => "fresh"}, ago(ctx.now, 60))

    assert {:ok, [%{"alert_issue_id" => "fresh"}]} =
             Queries.list(ctx.user,
               filters: %{"project" => "default"},
               since: ago(ctx.now, 7 * 86_400)
             )
  end

  test "clamps since to the 30 day retention window", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "too-old"}, ago(ctx.now, 40 * 86_400))

    assert {:ok, []} =
             Queries.list(ctx.user,
               filters: %{"project" => "default"},
               since: ago(ctx.now, 90 * 86_400)
             )
  end

  test "requires at least one filter and rejects unsafe keys", ctx do
    assert {:error, :filters_required} = Queries.list(ctx.user, filters: %{})

    assert {:error, {:invalid_filter_key, "project'; drop"}} =
             Queries.list(ctx.user, filters: %{"project'; drop" => "x"})
  end

  test "binds filter values as parameters", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "a"}, ago(ctx.now, 60))

    assert {:ok, []} = Queries.list(ctx.user, filters: %{"project" => "default' or '1'='1"})
  end

  test "get/3 returns one alert issue's latest version within the filters", ctx do
    insert_snapshot(ctx, %{"alert_issue_id" => "a", "status" => "open"}, ago(ctx.now, 120))
    insert_snapshot(ctx, %{"alert_issue_id" => "a", "status" => "resolved"}, ago(ctx.now, 60))

    assert {:ok, %{"alert_issue_id" => "a", "status" => "resolved"}} =
             Queries.get(ctx.user, "a", filters: %{"project" => "default"})

    assert {:error, :not_found} = Queries.get(ctx.user, "a", filters: %{"project" => "other"})
  end

  test "reads alert issues written by apply_signal through the ingest pipeline", ctx do
    start_supervised!(Logflare.SystemMetrics.AllLogsLogged)

    {:ok, alert} =
      Alerting.create_alert_query(ctx.user, %{
        name: "Postgres deadlocks",
        cron: "*/5 * * * *",
        query: "select 1",
        language: :pg_sql,
        rule_key: "user.default.deadlocks"
      })

    {:ok, %{snapshot: snapshot}} =
      AlertIssues.apply_signal(alert, %{
        fingerprint: "fp-e2e",
        status: :firing,
        identifiers: %{"project" => "default"}
      })

    TestUtils.retry_assert(fn ->
      assert {:ok, [issue]} = Queries.list(ctx.user, filters: %{"project" => "default"})
      assert issue["alert_issue_id"] == snapshot["alert_issue_id"]
      assert issue["title"] == "Postgres deadlocks"
    end)
  end

  describe "unread state" do
    test "alert issues without a read are unread", ctx do
      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "attention_at" => DateTime.to_iso8601(ago(ctx.now, 300))},
        ago(ctx.now, 300)
      )

      assert unread_by_id(ctx) == %{"a" => true}
    end

    test "a read after attention_at marks the alert issue read", ctx do
      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "attention_at" => DateTime.to_iso8601(ago(ctx.now, 300))},
        ago(ctx.now, 300)
      )

      insert_read(ctx, "a", true, ago(ctx.now, 200))
      assert unread_by_id(ctx) == %{"a" => false}
    end

    test "attention moving past the last read makes it unread again", ctx do
      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "attention_at" => DateTime.to_iso8601(ago(ctx.now, 300))},
        ago(ctx.now, 300)
      )

      insert_read(ctx, "a", true, ago(ctx.now, 200))

      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "attention_at" => DateTime.to_iso8601(ago(ctx.now, 100))},
        ago(ctx.now, 100)
      )

      assert unread_by_id(ctx) == %{"a" => true}
    end

    test "an explicit unread wins over an earlier read", ctx do
      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "attention_at" => DateTime.to_iso8601(ago(ctx.now, 300))},
        ago(ctx.now, 300)
      )

      insert_read(ctx, "a", true, ago(ctx.now, 200))
      insert_read(ctx, "a", false, ago(ctx.now, 100))
      assert unread_by_id(ctx) == %{"a" => true}
    end

    test "lists without a reader carry no unread flag", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a"}, ago(ctx.now, 300))
      assert {:ok, [issue]} = Queries.list(ctx.user, filters: %{"project" => "default"})
      refute Map.has_key?(issue, "unread")
    end
  end

  describe "before the system source tables exist" do
    defp drop!(%{backend: backend}, source) do
      {:ok, _} =
        PostgresAdaptor.SharedRepo.with_repo(backend, fn ->
          PostgresAdaptor.SharedRepo.query(
            "drop table if exists #{PostgresAdaptor.table_name(source)}",
            []
          )
        end)
    end

    test "treats a missing reads table as no reads", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a"}, ago(ctx.now, 60))
      drop!(ctx, ctx.reads_source)

      assert {:ok, [%{"alert_issue_id" => "a", "unread" => true}]} =
               Queries.list(ctx.user, filters: %{"project" => "default"}, reader: "user_1")
    end

    test "returns no alert issues when the alert issues table does not exist yet", ctx do
      drop!(ctx, ctx.source)

      assert {:ok, []} = Queries.list(ctx.user, filters: %{"project" => "default"})
      assert {:error, :not_found} = Queries.get(ctx.user, "a", filters: %{"project" => "default"})
    end
  end

  describe "silences" do
    test "hides silenced alert issues unless include_silenced is set", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a", "rule_key" => "r1"}, ctx.now)
      insert_snapshot(ctx, %{"alert_issue_id" => "b", "rule_key" => "r2"}, ctx.now)

      {:ok, _} =
        Silences.create(ctx.user, %{
          "rule_key" => "r1",
          "matchers" => %{"project" => "default"}
        })

      assert {:ok, [%{"alert_issue_id" => "b", "silenced" => false}]} =
               Queries.list(ctx.user, filters: %{"project" => "default"})

      assert {:ok, issues} =
               Queries.list(ctx.user, filters: %{"project" => "default"}, include_silenced: true)

      assert %{"a" => true, "b" => false} ==
               Map.new(issues, &{&1["alert_issue_id"], &1["silenced"]})
    end
  end

  describe "latest_by_fingerprint/2" do
    test "returns the newest version of one fingerprint", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a", "fingerprint" => "fp-1"}, ago(ctx.now, 60))

      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "fingerprint" => "fp-1", "status" => "resolved"},
        ago(ctx.now, 30)
      )

      insert_snapshot(ctx, %{"alert_issue_id" => "b", "fingerprint" => "fp-2"}, ago(ctx.now, 10))

      assert {:ok, %{"alert_issue_id" => "a", "status" => "resolved"}} =
               Queries.latest_by_fingerprint(ctx.user.id, "fp-1")
    end

    test "returns nil when the fingerprint has no stored alert issue", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "b", "fingerprint" => "fp-2"}, ctx.now)

      assert {:ok, nil} = Queries.latest_by_fingerprint(ctx.user.id, "fp-1")
    end

    test "returns nil before the alert issues table exists", ctx do
      {:ok, _} =
        PostgresAdaptor.SharedRepo.with_repo(ctx.backend, fn ->
          PostgresAdaptor.SharedRepo.query(
            "drop table if exists #{PostgresAdaptor.table_name(ctx.source)}",
            []
          )
        end)

      assert {:ok, nil} = Queries.latest_by_fingerprint(ctx.user.id, "fp-1")
    end
  end

  describe "operator views" do
    test "list_for_rule/2 lists one rule's alert issues across all identifiers", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a", "rule_key" => "r1"}, ago(ctx.now, 60))

      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "a", "rule_key" => "r1", "status" => "resolved"},
        ago(ctx.now, 30)
      )

      insert_snapshot(
        ctx,
        %{
          "alert_issue_id" => "b",
          "rule_key" => "r1",
          "identifiers" => %{"project" => "other"}
        },
        ago(ctx.now, 10)
      )

      insert_snapshot(ctx, %{"alert_issue_id" => "c", "rule_key" => "r2"}, ago(ctx.now, 10))

      {:ok, _} =
        Silences.create(ctx.user, %{"rule_key" => "r1", "matchers" => %{"project" => "other"}})

      assert {:ok, issues} = Queries.list_for_rule(ctx.user, "r1")

      assert Enum.map(issues, &{&1["alert_issue_id"], &1["status"], &1["silenced"]}) == [
               {"b", "open", true},
               {"a", "resolved", false}
             ]
    end

    test "open_counts/1 counts unresolved alert issues per rule", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a", "rule_key" => "r1"}, ago(ctx.now, 60))

      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "b", "rule_key" => "r1", "identifiers" => %{"project" => "x"}},
        ago(ctx.now, 60)
      )

      insert_snapshot(
        ctx,
        %{"alert_issue_id" => "c", "rule_key" => "r2", "status" => "resolved"},
        ago(ctx.now, 60)
      )

      assert {:ok, %{"r1" => 2}} == Queries.open_counts(ctx.user)
    end

    test "operator views are empty before the alert issues table exists", ctx do
      drop!(ctx, ctx.source)

      assert {:ok, []} == Queries.list_for_rule(ctx.user, "r1")
      assert {:ok, %{}} == Queries.open_counts(ctx.user)
    end
  end

  describe "comments/3" do
    defp insert_comment(%{comments_source: source, backend: backend}, attrs, at) do
      body =
        Map.merge(
          %{
            "alert_issue_id" => "a",
            "author" => "ops",
            "version" => DateTime.to_unix(at, :microsecond),
            "created_at" => DateTime.to_iso8601(at)
          },
          attrs
        )

      {:ok, _} =
        PostgresAdaptor.execute_query(
          backend,
          {"insert into #{PostgresAdaptor.table_name(source)} (id, body, event_message, timestamp) values ($1, $2, $3, $4) returning id",
           [Ecto.UUID.generate(), body, "comment", at]},
          []
        )
    end

    test "returns the latest version of each comment, oldest first", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a"}, ctx.now)
      insert_comment(ctx, %{"comment_id" => "c2", "body" => "second"}, ago(ctx.now, 10))
      insert_comment(ctx, %{"comment_id" => "c1", "body" => "first"}, ago(ctx.now, 60))

      insert_comment(
        ctx,
        %{
          "comment_id" => "c1",
          "body" => "first, edited",
          "created_at" => DateTime.to_iso8601(ago(ctx.now, 60))
        },
        ago(ctx.now, 5)
      )

      insert_comment(ctx, %{"alert_issue_id" => "b", "comment_id" => "c3"}, ago(ctx.now, 5))

      assert {:ok, [%{"comment_id" => "c1", "body" => "first, edited"}, %{"comment_id" => "c2"}]} =
               Queries.comments(ctx.user, "a", filters: %{"project" => "default"})
    end

    test "returns not found when the alert issue is outside the filters", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a"}, ctx.now)

      assert {:error, :not_found} =
               Queries.comments(ctx.user, "a", filters: %{"project" => "other"})
    end

    test "treats a missing comments table as no comments", ctx do
      insert_snapshot(ctx, %{"alert_issue_id" => "a"}, ctx.now)

      PostgresAdaptor.SharedRepo.with_repo(ctx.backend, fn ->
        PostgresAdaptor.SharedRepo.query(
          "drop table #{PostgresAdaptor.table_name(ctx.comments_source)}"
        )
      end)

      assert {:ok, []} = Queries.comments(ctx.user, "a", filters: %{"project" => "default"})
    end
  end
end
