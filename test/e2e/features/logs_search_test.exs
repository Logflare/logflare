defmodule E2e.Features.LogsSearchTest do
  use Logflare.FeatureCase, async: false

  alias Logflare.Backends
  alias Logflare.SingleTenant
  alias PlaywrightEx.Frame

  setup do
    start_supervised!(Logflare.SystemMetrics.AllLogsLogged)

    :ok
  end

  describe "logs search" do
    TestUtils.setup_single_tenant(seed_user: true, backend_type: :postgres)

    setup do
      user = SingleTenant.get_default_user()
      source = insert(:source, user: user)

      matching_message = "featuresearchmatch#{System.unique_integer([:positive])}"
      non_matching_message = "featuresearchmiss#{System.unique_integer([:positive])}"

      bq_schema = TestUtils.build_bq_schema(%{"event_message" => matching_message})
      insert(:source_schema, source: source, bigquery_schema: bq_schema)

      :ok = Backends.ensure_source_sup_started(source)

      {:ok, 2} =
        [
          build(:log_event, source: source, message: matching_message),
          build(:log_event, source: source, message: non_matching_message)
        ]
        |> Backends.ingest_logs(source)

      %{
        source: source,
        matching_message: matching_message,
        non_matching_message: non_matching_message
      }
    end

    test "searches logs from the search page", %{
      conn: conn,
      source: source,
      matching_message: matching_message,
      non_matching_message: non_matching_message
    } do
      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(
        ~p"/sources/#{source.id}/search?#{%{querystring: "event_message:#{matching_message}"}}"
      )
      |> assert_has("#logs-list-container", text: matching_message)
      |> refute_has("#logs-list-container", text: non_matching_message)
    end

    test "cancelling the datepicker resumes tailing", %{
      conn: conn,
      source: source
    } do
      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/sources/#{source.id}/search")
      |> assert_has(".live-pause", text: "Pause")
      |> click("#daterangepicker")
      |> wait_for_selector(".daterangepicker", state: "attached")
      |> click_date_range_cancel()
      |> assert_has(".live-pause", text: "Pause")
    end

    test "applying a preset date range updates the search query", %{
      conn: conn,
      source: source
    } do
      conn =
        conn
        |> visit(~p"/auth/login/single_tenant")
        |> assert_path(~p"/dashboard")
        |> visit(~p"/sources/#{source.id}/search")
        |> click("span", "DateTime")
        |> click_date_range_preset("Last 15 Minutes")
        |> assert_has(".live-pause", text: "Live")

      querystring =
        wait_for_editor_querystring(conn, "t:last@15")

      assert querystring =~ "t:last@15"
    end

    test "changing chart period updates the search query", %{
      conn: conn,
      source: source
    } do
      conn =
        conn
        |> visit(~p"/auth/login/single_tenant")
        |> assert_path(~p"/dashboard")
        |> visit(~p"/sources/#{source.id}/search")
        |> wait_for_selector("#source-logs-search-list")

      wait_for_editor_querystring(conn, "")

      conn
      |> unwrap(fn %{frame_id: frame_id} ->
        {:ok, _} =
          PlaywrightEx.Frame.select_option(frame_id,
            selector: "#search_chart_period",
            options: [%{label: "hour"}],
            timeout: 5_000
          )
      end)

      querystring =
        wait_for_editor_querystring(conn, "t::hour")

      assert querystring =~ "c:group_by(t::hour)"
    end
  end

  def wait_for_selector(conn, selector, opts \\ []) do
    opts = opts |> Keyword.merge(selector: selector, timeout: 10_000)

    conn
    |> unwrap(fn %{frame_id: frame_id} ->
      Frame.wait_for_selector(frame_id, opts)
    end)
  end

  defp click_date_range_preset(conn, preset) do
    trigger_click_event(conn, ~s|.daterangepicker .ranges li[data-range-key="#{preset}"]|)
  end

  defp click_date_range_cancel(conn) do
    trigger_click_event(conn, ".daterangepicker .cancelBtn")
  end

  defp trigger_click_event(conn, selector) do
    conn
    |> unwrap(fn %{frame_id: frame_id} ->
      {:ok, _event} =
        Frame.dispatch_event(frame_id,
          selector: selector,
          type: "click",
          event_init: %{bubbles: true, cancelable: true},
          timeout: 5_000
        )
    end)
  end

  defp wait_for_editor_querystring(conn, expected_fragment, timeout_ms \\ 10_000) do
    ref = make_ref()

    conn
    |> unwrap(fn %{frame_id: frame_id} ->
      {:ok, _} =
        Frame.wait_for_function(frame_id,
          expression: """
          ({ expectedFragment }) => {
            const querystring =
              document.querySelector("#lql-editor-hook")?.dataset.querystring ?? ""

            if (expectedFragment === "") return querystring !== ""

            return querystring.includes(expectedFragment)
          }
          """,
          is_function: true,
          arg: %{expectedFragment: expected_fragment},
          timeout: timeout_ms
        )

      {:ok, querystring} =
        Frame.evaluate(
          frame_id,
          expression: ~S|document.querySelector("#lql-editor-hook")?.dataset.querystring ?? ""|,
          timeout: 5_000
        )

      send(self(), {ref, querystring})
    end)

    assert_receive {^ref, querystring}
    querystring
  end
end
