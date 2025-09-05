defmodule LogflareWeb.SearchLive.EventContextComponentTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Sources.Source
  alias LogflareWeb.SearchLive.EventContextComponent

  @default_schema Source.BigQuery.SchemaBuilder.initial_table_schema()

  def setup_user(_) do
    plan = insert(:plan)
    user = insert(:user)
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    [user: user, plan: plan]
  end

  defp setup_mocks(context) do
    stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
      if opts[:body].query =~ "rank" do
        context[:bq_response]
      else
        {:ok, TestUtils.gen_bq_response()}
      end
    end)

    :ok
  end

  defp on_exit_kill_tasks(_ctx) do
    on_exit(fn ->
      # Kill all tasks first
      Logflare.Utils.Tasks.kill_all_tasks()

      # Give processes time to clean up
      Process.sleep(10)

      :ok
    end)

    :ok
  end

  defp setup_user_session(%{conn: conn, user: user}) do
    conn = conn |> put_session(:user_id, user.id) |> assign(:user, user)
    [conn: conn]
  end

  def wait_for_render(view, selector, timeout \\ 5000) do
    if view |> has_element?(selector) do
      view
    else
      receive do
        {:wait_for_render, _} ->
          wait_for_render(view, selector)
      after
        timeout ->
          raise "Timeout waiting for render"
      end
    end
  end

  describe "construct context query" do
    setup [:setup_user]

    setup %{user: user} do
      schema =
        %{"user_id" => 1, "metadata" => %{"level" => "info"}}
        |> Source.BigQuery.SchemaBuilder.build_table_schema(@default_schema)

      source = insert(:source, user: user, suggested_keys: "event_message")
      insert(:source_schema, source: source, bigquery_schema: schema)

      [
        source: source,
        schema: schema,
        timestamp: ~U[2025-08-20T02:33:51Z]
      ]
    end

    test "prepare_lql_rules/1", %{source: source, timestamp: timestamp} do
      query_string = "c:count(*) c:group_by(t::minute)"

      assert [
               %Logflare.Lql.Rules.FilterRule{
                 path: "timestamp",
                 operator: :range,
                 values: [~U[2025-08-19 02:33:51Z], ~U[2025-08-21 02:33:51Z]]
               }
             ] = EventContextComponent.prepare_lql_rules(source, query_string, timestamp)

      refute EventContextComponent.prepare_lql_rules(
               source,
               query_string <> " m.level:debug",
               timestamp
             )
             |> Enum.find(&(&1.path =~ "level"))
    end

    test "prepare_lql_rules/1 includes suggested_keys", %{source: source, timestamp: timestamp} do
      assert [
               %Logflare.Lql.Rules.FilterRule{
                 path: "event_message",
                 operator: :=,
                 value: "sign_in"
               },
               %Logflare.Lql.Rules.FilterRule{
                 path: "timestamp",
                 operator: :range,
                 values: [~U[2025-08-19 02:33:51Z], ~U[2025-08-21 02:33:51Z]]
               }
             ] =
               EventContextComponent.prepare_lql_rules(source, "event_message:sign_in", timestamp)
    end

    test "prepare_lql_rules/1 includes clustering_fields", %{
      user: user,
      schema: schema,
      timestamp: timestamp
    } do
      source = insert(:source, user: user, bigquery_clustering_fields: "user_id")
      insert(:source_schema, source: source, bigquery_schema: schema)

      assert [
               %Logflare.Lql.Rules.FilterRule{path: "user_id", operator: :=, value: 1},
               %Logflare.Lql.Rules.FilterRule{
                 path: "timestamp",
                 operator: :range,
                 values: [~U[2025-08-19 02:33:51Z], ~U[2025-08-21 02:33:51Z]]
               }
             ] = EventContextComponent.prepare_lql_rules(source, "user_id:1", timestamp)
    end
  end

  describe "supports legacy  partition types" do
    setup [:setup_user]

    test "search_logs/4 supports legacy table partitions", %{user: user} do
      pid = self()
      ref = make_ref()

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, {ref, opts[:body].query})

        {:ok, TestUtils.gen_bq_response()}
      end)

      [:pseudo, :timestamp]
      |> Enum.each(fn partition_type ->
        legacy_source = insert(:source, user: user, bq_table_partition_type: partition_type)
        insert(:source_schema, source: legacy_source, bigquery_schema: @default_schema)

        timestamp = ~U[2023-01-01 00:00:00Z]
        lql_rules = EventContextComponent.prepare_lql_rules(legacy_source, "", timestamp)

        EventContextComponent.search_logs(
          Ecto.UUID.generate(),
          timestamp,
          legacy_source.id,
          lql_rules
        )

        assert_receive {^ref, query}

        case partition_type do
          :pseudo ->
            assert query =~ "_PARTITIONTIME"
            assert query =~ "_PARTITIONDATE IS NULL"

          :timestamp ->
            refute query =~ "_PARTITIONTIME"
            refute query =~ "_PARTITIONDATE IS NULL"
        end
      end)
    end
  end

  describe "event context modal" do
    setup [:setup_user, :setup_mocks, :on_exit_kill_tasks, :setup_user_session]

    setup %{user: user} do
      [source: insert(:source, user: user)]
    end

    setup do
      parent = self()

      :telemetry.attach(
        "wait-for-render-#{System.unique_integer()}",
        [:phoenix, :live_view, :render, :stop],
        fn _event, _measurements, metadata, _config ->
          send(parent, {:wait_for_render, metadata.socket.assigns})
        end,
        nil
      )
    end

    @tag bq_response:
           {:ok,
            Enum.map(1..10, &%{"event_message" => "event message #{&1}"})
            |> TestUtils.gen_bq_response()}
    test "viewing event context", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search?tailing=false")

      html =
        view
        |> wait_for_render("#logs-list li:first-of-type a")
        |> element("#logs-list li:first-of-type a", "context")
        |> render_click()

      # Verify the context modal opens
      assert html =~ "View Event Context"

      view |> wait_for_render("#context_log_events #log-events li:first-of-type")

      assert view |> has_element?("#context_log_events #log-events li", "event message 1")
      assert view |> has_element?("#context_log_events #log-events li", "event message 10")
    end

    @tag bq_response: {:error, TestUtils.gen_bq_error("bad query")}
    test "renders the error message", %{conn: conn, source: source} do
      {:ok, view, _html} = live(conn, ~p"/sources/#{source.id}/search?tailing=false")

      view
      |> wait_for_render("#logs-list li:first-of-type a")
      |> element("#logs-list li:first-of-type a", "context")
      |> render_click()

      view
      |> wait_for_render("#context_log_events_error")

      assert view |> has_element?("#context_log_events", "An error occurred")
    end
  end
end
