defmodule LogflareWeb.SearchLive.EventContextComponentTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.SharedRepo
  alias Logflare.Logs.SearchOperation
  alias Logflare.Lql.Rules.FilterRule
  alias Logflare.SingleTenant
  alias Logflare.Sources.Source
  alias Logflare.Utils.Tasks
  alias LogflareWeb.SearchLive.EventContextComponent

  test "prepare_lql_rules/3 retains recommended filters and a day on either side of the event" do
    source = insert(:source, user: insert(:user), suggested_keys: nil)
    schema = TestUtils.build_bq_schema(%{"user_id" => 1, "metadata" => %{"level" => "info"}})
    insert(:source_schema, source: source, bigquery_schema: schema)

    timestamp = ~U[2025-08-20 02:33:51Z]
    query = "c:count(*) c:group_by(t::minute) m.level:debug event_message:sign_in user_id:1"

    assert [
             %FilterRule{
               path: "timestamp",
               operator: :range,
               values: [~U[2025-08-19 02:33:51Z], ~U[2025-08-21 02:33:51Z]]
             } = time_window
           ] = EventContextComponent.prepare_lql_rules(source, query, timestamp)

    for field <- ["event_message", "event_message!"] do
      suggested = %{source | suggested_keys: field}

      assert [%FilterRule{path: "event_message", operator: :=, value: "sign_in"}, ^time_window] =
               EventContextComponent.prepare_lql_rules(suggested, query, timestamp)
    end

    clustered = %{source | bigquery_clustering_fields: "user_id"}

    assert [%FilterRule{path: "user_id", operator: :=, value: 1}, ^time_window] =
             EventContextComponent.prepare_lql_rules(clustered, query, timestamp)
  end

  test "search_logs/4 restricts legacy BigQuery partitions" do
    insert(:plan)
    user = insert(:user)
    timestamp = ~U[2023-01-01 00:00:00Z]
    parent = self()

    for partition_type <- [:pseudo, :timestamp] do
      source = insert(:source, user: user, bq_table_partition_type: partition_type)
      rules = EventContextComponent.prepare_lql_rules(source, "", timestamp)

      expect(GoogleApi.BigQuery.V2.Api.Jobs, :bigquery_jobs_query, fn _conn, _project, opts ->
        send(parent, {:context_query, partition_type, opts[:body].query})
        {:ok, TestUtils.gen_bq_response()}
      end)

      assert %{rows: [_]} =
               EventContextComponent.search_logs(
                 Ecto.UUID.generate(),
                 timestamp,
                 source.id,
                 rules
               )

      assert_receive {:context_query, ^partition_type, query}
      assert String.contains?(query, "_PARTITIONTIME") == (partition_type == :pseudo)
      assert String.contains?(query, "_PARTITIONDATE IS NULL") == (partition_type == :pseudo)
    end
  end

  describe "event context modal" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    setup {TestUtils, :attach_wait_for_render}

    setup %{conn: conn} = context do
      start_supervised!(Logflare.SystemMetricsSup)
      on_exit(&Tasks.kill_all_tasks/0)

      user = SingleTenant.get_default_user()
      source = insert(:source, user: user)
      target_id = Ecto.UUID.generate()
      timestamp = DateTime.add(DateTime.utc_now(), -3, :minute) |> DateTime.to_unix(:microsecond)

      events =
        for rank <- Map.get(context, :context_ranks, 0..0) do
          %{
            "id" => if(rank == 0, do: target_id, else: Ecto.UUID.generate()),
            "timestamp" => timestamp + rank * 1_000_000,
            "event_message" => "context event #{rank}",
            "metadata" => %{"position" => rank}
          }
        end

      insert(:source_schema,
        source: source,
        bigquery_schema: TestUtils.build_bq_schema(hd(events))
      )

      :ok = Backends.ensure_source_sup_started(source)
      {:ok, _} = Backends.ingest_logs(events, source)
      TestUtils.wait_for_postgres_events(source, user, "context event", length(events))

      params = %{querystring: ~s(event_message:"context event 0"), tailing?: false}

      {:ok, view, _html} =
        live_with_redirect(login_user(conn, user), ~p"/sources/#{source.id}/search?#{params}")

      selector = "#logs-list li[data-event-id='#{target_id}'] a"
      TestUtils.wait_for_render(view, selector)

      [
        view: view,
        link: element(view, selector, "context"),
        target_id: target_id,
        source: source,
        timestamp: timestamp
      ]
    end

    @tag context_ranks: -125..125
    test "keeps ordered context across pages, new arrivals and parent updates",
         %{view: view, source: source} = context do
      open_context(context)
      assert_context_events(view, -50..50)

      late_events =
        for offset <- [-49_500_000, 49_500_000] do
          %{"timestamp" => context.timestamp + offset, "event_message" => "late event #{offset}"}
        end

      {:ok, 2} = Backends.ingest_logs(late_events, source)
      TestUtils.wait_for_postgres_events(source, source.user, "late event", 2)

      for {direction, ranks} <- [
            before: -100..50,
            after: -100..100,
            before: -125..100,
            after: -125..125
          ] do
        load_more(view, direction)
        assert_context_events(view, ranks)
      end

      aggregates = SearchOperation.new(%{source: source, lql_rules: [], type: :aggregates})
      send(view.pid, {:search_result, %{aggregates: aggregates}})
      render_async(view, 5_000)

      assert_context_events(view, -125..125)
      assert has_element?(view, ".tw-hidden > #context-load-more-before[disabled]")
      assert has_element?(view, ".tw-hidden > #context-load-more-after[disabled]")
      assert has_element?(view, "#metadata-#{context.target_id}", ~s("position": 0))
    end

    test "shows empty context when the selected event is missing", %{view: view} = context do
      open_context(context, %{"log-event-id" => Ecto.UUID.generate()})
      assert_context_events(view, [])
      refute has_element?(view, "#context_log_events button:not([disabled])")
    end

    @tag :capture_log
    @tag context_ranks: -100..50
    test "preserves context on a page error and retries from the same cursor",
         %{view: view, source: source} = context do
      open_context(context)
      assert has_element?(view, ".tw-hidden > #context-load-more-after[disabled]")

      assert :ok =
               with_unavailable_events_table(source, fn ->
                 load_more(view, :before)

                 assert has_element?(
                          view,
                          "#context_load_more_error[role=alert]",
                          "Unable to load more events"
                        )

                 refute has_element?(view, "#context_log_events #context_load_more_error")
                 assert_context_events(view, -50..50)
                 assert has_element?(view, "#context-load-more-before:not([disabled])")
               end)

      load_more(view, :before)
      assert_context_events(view, -100..50)
      refute has_element?(view, "#context_load_more_error")
      assert has_element?(view, ".tw-hidden > #context-load-more-before[disabled]")
    end

    @tag :capture_log
    test "shows an error when the initial context query fails",
         %{view: view, source: source} = context do
      assert :ok =
               with_unavailable_events_table(source, fn ->
                 open_context(context)
                 assert has_element?(view, "#context_log_events_error", "An error occurred")
               end)
    end
  end

  @spec with_unavailable_events_table(Source.t(), (-> term())) ::
          :ok | {:error, :cannot_connect}
  defp with_unavailable_events_table(source, fun) do
    backend = SingleTenant.get_default_backend()
    table = PostgresAdaptor.table_name(source)

    SharedRepo.with_repo(backend, fn ->
      SharedRepo.query!("ALTER TABLE #{table} RENAME TO #{table}_unavailable")

      try do
        fun.()
        :ok
      after
        SharedRepo.query!("ALTER TABLE #{table}_unavailable RENAME TO #{table}")
      end
    end)
  end

  @spec open_context(map(), map()) :: String.t()
  defp open_context(%{view: view, link: link}, params \\ %{}) do
    render_click(link, params)
    render_async(view, 5_000)
  end

  @spec load_more(map(), :before | :after) :: String.t()
  defp load_more(view, direction) do
    view
    |> element("#context-load-more-#{direction}:not([disabled])", "Load 50 more")
    |> render_click()

    render_async(view, 5_000)
  end

  @spec assert_context_events(map(), Range.t() | [integer()]) :: true
  defp assert_context_events(view, ranks) do
    messages =
      view
      |> element("#context_log_events #log-events")
      |> render()
      |> Floki.parse_fragment!()
      |> Floki.find("li > span.tw-truncate")
      |> Enum.map(fn message -> message |> Floki.text() |> String.trim() end)

    assert messages == Enum.map(ranks, &"context event #{&1}")
  end
end
