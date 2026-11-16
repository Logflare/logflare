defmodule Logflare.AlertIssues.QueriesTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues
  alias Logflare.AlertIssues.Queries
  alias Logflare.Alerting
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Sources

  setup do
    insert(:plan, name: "Free", limit_source_ttl: :timer.hours(24 * 90))
    user = insert(:user)
    sources = Sources.create_alert_issue_system_sources(user.id)
    source = Enum.find(sources, &(&1.system_source_type == :alert_issues))

    cfg = Application.get_env(:logflare, Logflare.Repo)
    url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

    backend =
      insert(:backend, type: :postgres, config: %{url: url}, sources: [source], user: user)

    PostgresAdaptor.create_repo(backend)
    PostgresAdaptor.create_events_table({source, backend})
    on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)

    now = DateTime.utc_now()
    %{user: user, source: source, backend: backend, now: now}
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
end
