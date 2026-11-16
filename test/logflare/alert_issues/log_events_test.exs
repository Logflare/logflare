defmodule Logflare.AlertIssues.LogEventsTest do
  use Logflare.DataCase, async: false

  alias Logflare.AlertIssues.LogEvents
  alias Logflare.AlertIssues.Queries
  alias Logflare.Alerting
  alias Logflare.Backends.Adaptor.PostgresAdaptor

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user, name: "edge_logs_demo")
    other = insert(:source, user: insert(:user), name: "someone_elses")

    cfg = Application.get_env(:logflare, Logflare.Repo)
    url = "postgresql://#{cfg[:username]}:#{cfg[:password]}@#{cfg[:hostname]}/#{cfg[:database]}"

    backend =
      insert(:backend, type: :postgres, config: %{url: url}, sources: [source], user: user)

    PostgresAdaptor.create_repo(backend)
    PostgresAdaptor.create_events_table({source, backend})
    on_exit(fn -> PostgresAdaptor.destroy_instance({source, backend}) end)

    now = DateTime.utc_now()

    insert_event = fn message, metadata, at ->
      {:ok, _} =
        PostgresAdaptor.execute_query(
          backend,
          {"insert into #{PostgresAdaptor.table_name(source)} (id, body, event_message, timestamp) values ($1, $2, $3, $4) returning id",
           [
             Ecto.UUID.generate(),
             %{"event_message" => message, "metadata" => metadata},
             message,
             at
           ]},
          []
        )
    end

    for n <- 1..3,
        do:
          insert_event.(
            "POST /functions/v1/stripe-webhook 500",
            %{"function_id" => "stripe-webhook", "status" => 500},
            DateTime.add(now, -n * 60, :second)
          )

    insert_event.(
      "POST /functions/v1/send-email 503",
      %{"function_id" => "send-email", "status" => 503},
      DateTime.add(now, -120, :second)
    )

    insert_event.(
      "POST /functions/v1/hello 200",
      %{"function_id" => "hello", "status" => 200},
      DateTime.add(now, -30, :second)
    )

    insert_event.(
      "old stripe failure",
      %{"function_id" => "stripe-webhook", "status" => 500},
      DateTime.add(now, -3, :hour)
    )

    %{user: user, other: other, now: now, range: [from: DateTime.add(now, -1, :hour), to: now]}
  end

  describe "preview/3" do
    test "filters events on metadata fields within the time range", %{user: user, range: range} do
      assert {:ok, result} =
               LogEvents.preview(
                 user,
                 ~s(f:edge_logs_demo m.function_id:"stripe-webhook" m.status:>=500),
                 range
               )

      assert result.source == "edge_logs_demo"
      assert result.total == 3
      assert length(result.events) == 3

      assert Enum.all?(
               result.events,
               &(&1["body"]["metadata"]["function_id"] == "stripe-webhook")
             )

      timestamps = Enum.map(result.events, &DateTime.to_unix(&1["timestamp"], :microsecond))
      assert timestamps == Enum.sort(timestamps, :desc)
      assert Enum.sum(Enum.map(result.buckets, & &1.count)) == 3
    end

    test "supports ranges, negation and free-text search", %{user: user, range: range} do
      assert {:ok, %{total: 4}} =
               LogEvents.preview(user, "f:edge_logs_demo m.status:500..599", range)

      assert {:ok, %{total: 1}} =
               LogEvents.preview(
                 user,
                 "f:edge_logs_demo -m.function_id:stripe-webhook 503",
                 range
               )

      assert {:ok, %{total: 1}} = LogEvents.preview(user, "f:edge_logs_demo hello", range)
      assert {:ok, %{total: 1}} = LogEvents.preview(user, "f:edge_logs_demo m.status:<500", range)
    end

    test "uses hourly buckets for ranges longer than 3 hours", %{user: user, now: now} do
      assert {:ok, %{total: 4, buckets: buckets}} =
               LogEvents.preview(user, "f:edge_logs_demo m.function_id:\"stripe-webhook\"",
                 from: DateTime.add(now, -24, :hour),
                 to: now
               )

      assert Enum.all?(buckets, &(&1.timestamp.minute == 0))
    end

    test "rejects LQL without a source, with an unknown source or another user's source", %{
      user: user,
      range: range
    } do
      assert {:error, :source_required} = LogEvents.preview(user, "m.status:500", range)
      assert {:error, {:unknown_source, "nope"}} = LogEvents.preview(user, "f:nope", range)

      assert {:error, {:unknown_source, "someone_elses"}} =
               LogEvents.preview(user, "f:someone_elses", range)
    end

    test "binds values as parameters", %{user: user, range: range} do
      assert {:ok, %{total: 0}} =
               LogEvents.preview(
                 user,
                 ~s(f:edge_logs_demo m.function_id:"x'; drop table users; --"),
                 range
               )
    end
  end

  describe "fill_template/2" do
    test "quotes identifier values into the LQL" do
      assert {:ok, ~s(f:edge_logs_demo m.function_id:"stripe-webhook")} =
               LogEvents.fill_template("f:edge_logs_demo m.function_id:{{function_id}}", %{
                 "function_id" => "stripe-webhook"
               })
    end

    test "rejects missing identifiers and values containing quotes" do
      assert {:error, {:missing_identifier, "function_id"}} =
               LogEvents.fill_template("m.function_id:{{function_id}}", %{})

      assert {:error, {:invalid_identifier, "function_id"}} =
               LogEvents.fill_template("m.function_id:{{function_id}}", %{
                 "function_id" => ~s(a" OR m.x:1)
               })
    end
  end

  describe "clamp_range/2" do
    test "defaults to the last 24 hours and clamps to 7 days", %{now: now} do
      {from, to} = LogEvents.clamp_range(nil, nil)
      assert DateTime.diff(to, from, :hour) == 24

      {from, to} = LogEvents.clamp_range(DateTime.add(now, -20, :day), now)
      assert DateTime.diff(to, from, :day) == 7
    end
  end

  describe "aggregate/4" do
    test "returns one row per group with counts and samples", %{user: user, now: now} do
      assert {:ok, rows} =
               LogEvents.aggregate(
                 user,
                 "f:edge_logs_demo m.status:>=500",
                 ["metadata.function_id"],
                 DateTime.add(now, -1, :hour)
               )

      by_function = Map.new(rows, &{&1["metadata.function_id"], &1})
      assert Map.keys(by_function) |> Enum.sort() == ["send-email", "stripe-webhook"]
      assert by_function["stripe-webhook"]["count"] == 3
      assert length(by_function["stripe-webhook"]["sample_messages"]) == 3
      assert is_binary(by_function["stripe-webhook"]["last_seen"])
    end

    test "without group_by returns one row only when events matched", %{user: user, now: now} do
      since = DateTime.add(now, -1, :hour)

      assert {:ok, [%{"count" => 4}]} =
               LogEvents.aggregate(user, "f:edge_logs_demo m.status:>=500", [], since)

      assert {:ok, []} = LogEvents.aggregate(user, "f:edge_logs_demo m.status:>=600", [], since)
    end
  end

  describe "for_alert_issue/3" do
    defp create_alert(user, attrs) do
      {:ok, alert} =
        Alerting.create_alert_query(
          user,
          Map.merge(
            %{
              name: "Edge function errors",
              cron: "*/5 * * * *",
              query: "select 1",
              language: :pg_sql,
              rule_key: "logflare.edge-fn-5xx"
            },
            attrs
          )
        )

      alert
    end

    defp stub_issue(identifiers) do
      stub(Queries, :get, fn _user, "a1", opts ->
        assert opts[:filters] == %{"project" => "default"}

        {:ok,
         %{
           "alert_issue_id" => "a1",
           "rule_key" => "logflare.edge-fn-5xx",
           "identifiers" => identifiers
         }}
      end)
    end

    test "fills the rule's preview LQL with the alert issue identifiers", %{
      user: user,
      range: range
    } do
      create_alert(user, %{
        signal_mapping: %{
          "preview" => %{"lql" => "f:edge_logs_demo m.function_id:{{function_id}} m.status:>=500"}
        }
      })

      stub_issue(%{"project" => "default", "function_id" => "stripe-webhook"})

      assert {:ok, %{total: 3, lql: lql}} =
               LogEvents.for_alert_issue(
                 user,
                 "a1",
                 [filters: %{"project" => "default"}] ++ range
               )

      assert lql =~ ~s(m.function_id:"stripe-webhook")
    end

    test "returns no_preview when the rule has no preview", %{user: user, range: range} do
      create_alert(user, %{})
      stub_issue(%{"project" => "default"})

      assert {:error, :no_preview} =
               LogEvents.for_alert_issue(
                 user,
                 "a1",
                 [filters: %{"project" => "default"}] ++ range
               )
    end
  end
end
