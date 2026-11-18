defmodule LogflareWeb.SearchLive.EventContextComponentTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.QueryResult
  alias Logflare.BqRepo
  alias Logflare.Logs.SearchOperation
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.Sources.Source
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias Logflare.Utils.Tasks
  alias LogflareWeb.SearchLive.EventContextComponent
  alias Logflare.Users

  @default_schema Source.BigQuery.SchemaBuilder.initial_table_schema()

  def setup_user(_) do
    plan = insert(:plan)
    user = insert(:user)
    team = insert(:team, user: user)
    _billing_account = insert(:billing_account, user: user, stripe_plan_id: plan.stripe_id)
    [user: user, team: team, plan: plan]
  end

  defp setup_mocks(context) do
    stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
      if opts[:body].query =~ "rank" do
        context_response(context, opts[:body].queryParameters)
      else
        {:ok, TestUtils.gen_bq_response()}
      end
    end)

    stub(BigQueryAdaptor, :execute_query, fn identifier, query, opts ->
      handle_bigquery_execute_query(identifier, query, opts)
    end)

    :ok
  end

  @spec context_response(map(), [map()]) :: {:ok, struct()} | {:error, term()}
  defp context_response(%{context_ranks: ranks}, params) do
    [first, last] = params |> Enum.take(-2) |> Enum.map(& &1.parameterValue.value)

    rows =
      for rank <- ranks, rank in first..last do
        %{"event_message" => "context event #{rank}", "rank" => to_string(rank)}
      end

    {:ok,
     TestUtils.gen_bq_response(
       rows,
       SchemaBuilder.build_table_schema(%{"rank" => 1}, @default_schema)
     )}
  end

  defp context_response(context, _params), do: context.bq_response

  defp handle_bigquery_execute_query({project_id, dataset_id, user_id}, query, opts) do
    user = Users.get(user_id)

    with {:ok, {bq_sql, bq_params}} <- BigQueryAdaptor.ecto_to_sql(query, opts) do
      bq_sql = String.replace(bq_sql, "$$__DEFAULT_DATASET__$$", dataset_id)

      # Call BqRepo with our converted query - this will hit our GoogleApi mock
      case BqRepo.query_with_sql_and_params(
             user,
             project_id,
             bq_sql,
             bq_params,
             [dataset_id: dataset_id] ++ opts
           ) do
        {:ok, result} ->
          {:ok,
           QueryResult.new(result.rows, %{
             total_bytes_processed: result.total_bytes_processed,
             total_rows: result.total_rows,
             query_string: bq_sql,
             bq_params: bq_params
           })}

        error ->
          error
      end
    end
  end

  defp on_exit_kill_tasks(_ctx) do
    on_exit(fn ->
      # Kill all tasks first
      Tasks.kill_all_tasks()

      # Give processes time to clean up
      Process.sleep(10)

      :ok
    end)

    :ok
  end

  defp setup_user_session(%{conn: conn, user: user}) do
    [conn: login_user(conn, user)]
  end

  describe "construct context query" do
    setup [:setup_user]

    setup %{user: user} do
      schema =
        %{"user_id" => 1, "metadata" => %{"level" => "info"}}
        |> SchemaBuilder.build_table_schema(@default_schema)

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
               %FilterRule{
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

    test "prepare_lql_rules/1 handles required marker in suggested_keys", %{
      user: user,
      schema: schema,
      timestamp: timestamp
    } do
      source = insert(:source, user: user, suggested_keys: "event_message!")
      insert(:source_schema, source: source, bigquery_schema: schema)

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

    test "prepare_lql_rules/1 handles nil suggested_keys", %{
      user: user,
      schema: schema,
      timestamp: timestamp
    } do
      source = insert(:source, user: user, suggested_keys: nil)
      insert(:source_schema, source: source, bigquery_schema: schema)

      assert [
               %Logflare.Lql.Rules.FilterRule{
                 path: "timestamp",
                 operator: :range,
                 values: [~U[2025-08-19 02:33:51Z], ~U[2025-08-21 02:33:51Z]]
               }
             ] = EventContextComponent.prepare_lql_rules(source, "", timestamp)
    end
  end

  describe "supports legacy  partition types" do
    setup [:setup_user]

    test "search_logs/4 supports legacy table partitions", %{user: user} do
      pid = self()
      ref = make_ref()

      stub(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _proj_id, opts ->
        send(pid, {ref, opts[:body]})

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

        assert_receive {^ref, request}

        query = request.query

        assert request.queryParameters |> Enum.take(-2) |> Enum.map(& &1.parameterValue.value) ==
                 [-50, 50]

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

    setup {TestUtils, :attach_wait_for_render}

    @tag context_ranks: -75..75
    test "viewing event context", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live_with_redirect(conn, ~p"/sources/#{source.id}/search?tailing=false")

      html =
        view
        |> TestUtils.wait_for_render("#logs-list li[data-event-id] a")
        |> element("#logs-list li[data-event-id] a", "context")
        |> render_click()

      assert html =~ "View Event Context"

      view |> TestUtils.wait_for_render("#context_log_events #log-events li:first-of-type")

      assert has_element?(
               view,
               "#context_log_events #log-events li:first-child",
               "context event -50"
             )

      assert has_element?(
               view,
               "#context_log_events #log-events li:last-child",
               "context event 50"
             )

      assert has_element?(view, "#context-load-more-before:not([disabled])", "Load 50 more")
      assert has_element?(view, "#context-load-more-after:not([disabled])", "Load 50 more")
      assert has_element?(view, "#log-events > li:nth-child(101)")
      refute has_element?(view, "#context_log_events #log-events li:nth-child(102)")

      expect(BigQueryAdaptor, :execute_query, fn _, _, _ -> {:error, "query failed"} end)
      view |> element("#context-load-more-before") |> render_click()
      render_async(view)

      assert has_element?(
               view,
               "#context_load_more_error[role=alert]",
               "Unable to load more events"
             )

      refute has_element?(view, "#context_log_events #context_load_more_error")
      assert has_element?(view, "#context_log_events #log-events li:nth-child(101)")
      assert has_element?(view, "#context-load-more-before:not([disabled])")

      view |> element("#context-load-more-before") |> render_click()
      render_async(view)

      assert has_element?(view, "#log-events > li:first-child", "context event -75")
      assert has_element?(view, "#log-events > li:nth-child(126)", "context event 50")
      assert has_element?(view, "div.tw-hidden > #context-load-more-before[disabled]")
      assert has_element?(view, "#context-load-more-after:not([disabled])")
      refute has_element?(view, "#context_load_more_error")

      aggregates =
        struct!(SearchOperation, :sys.get_state(view.pid).socket.assigns.search_op_log_aggregates)

      send(view.pid, {:search_result, %{aggregates: %{aggregates | rows: []}}})
      render_async(view)

      assert has_element?(view, "#log-events > li:first-child", "context event -75")
      assert has_element?(view, "#log-events > li:nth-child(126)", "context event 50")

      view |> element("#context-load-more-after") |> render_click()
      render_async(view)

      assert has_element?(view, "#log-events > li:first-child", "context event -75")
      assert has_element?(view, "#log-events > li:nth-child(151)", "context event 75")
      refute has_element?(view, "#log-events > li:nth-child(152)")
      assert has_element?(view, "#log-events > li", "context event 0")
      assert has_element?(view, "div.tw-hidden > #context-load-more-before[disabled]")
      assert has_element?(view, "div.tw-hidden > #context-load-more-after[disabled]")
    end

    @tag bq_response: {:error, TestUtils.gen_bq_error("bad query")}
    test "renders the error message", %{conn: conn, source: source} do
      {:ok, view, _html} =
        live_with_redirect(conn, ~p"/sources/#{source.id}/search?tailing=false")

      view
      |> TestUtils.wait_for_render("#logs-list li[data-event-id] a")
      |> element("#logs-list li[data-event-id] a", "context")
      |> render_click()

      view
      |> TestUtils.wait_for_render("#context_log_events_error")

      assert view |> has_element?("#context_log_events", "An error occurred")
    end
  end
end
