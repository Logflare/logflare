defmodule E2e.Features.LogsSearchTest do
  use Logflare.FeatureCase, async: false

  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.NaturalLanguageLql.AnthropicClient
  alias Logflare.Repo
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

      bq_schema =
        TestUtils.build_bq_schema(%{
          "event_message" => matching_message,
          "metadata" => %{"level" => "warning", "response" => %{"status_code" => 200}}
        })

      insert(:source_schema, source: source, bigquery_schema: bq_schema)

      :ok = Backends.ensure_source_sup_started(source)

      {:ok, 2} =
        [
          build(:log_event,
            source: source,
            message: matching_message,
            metadata: %{"level" => "warning", "response" => %{"status_code" => 200}}
          ),
          build(:log_event,
            source: source,
            message: non_matching_message,
            metadata: %{"level" => "warning", "response" => %{"status_code" => 200}}
          )
        ]
        |> Backends.ingest_logs(source)

      %{
        source: source,
        user: user,
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

    test "Cmd/Ctrl+Enter submits the editor value to AI Assist", %{
      conn: conn,
      source: source
    } do
      parent = self()

      source =
        source
        |> Changeset.change(suggested_keys: "metadata.level")
        |> Repo.update!()

      stub(AnthropicClient, :configured?, fn -> true end)

      stub(AnthropicClient, :generate, fn prompt ->
        send(parent, {:anthropic_prompt, prompt})

        {:ok,
         %{
           text: ~s({"kind":"query","lql":"event_message:error","error":null}),
           request_id: "request-e2e-shortcut"
         }}
      end)

      conn =
        conn
        |> visit(~p"/auth/login/single_tenant")
        |> assert_path(~p"/dashboard")
        |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "warning"}}")
        |> assert_has("#ai-search-button")
        |> fill_in("metadata.level", with: " error ")
        |> wait_for_selector(".monaco-editor textarea.inputarea")

      conn = press(conn, ".monaco-editor textarea.inputarea", "Control+Enter")

      assert_receive {:anthropic_prompt, prompt}, 5_000
      assert prompt =~ "Natural-language request:\nwarning"

      querystring = wait_for_editor_querystring(conn, ~s|~"(?i)error"|)
      assert querystring =~ ~s|~"(?i)error"|
      assert querystring =~ "m.level:error"
    end

    test "Enter submits a regular search from the editor", %{
      conn: conn,
      source: source,
      non_matching_message: non_matching_message
    } do
      stub(AnthropicClient, :configured?, fn -> true end)
      reject(AnthropicClient, :generate, 1)

      conn =
        conn
        |> visit(~p"/auth/login/single_tenant")
        |> assert_path(~p"/dashboard")
        |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "warning"}}")
        |> wait_for_selector(".monaco-editor textarea.inputarea")
        |> fill_editor("event_message:#{non_matching_message}")
        |> press(".monaco-editor textarea.inputarea", "Enter")

      assert wait_for_editor_querystring(conn, non_matching_message) =~ non_matching_message
    end

    test "Tab focuses the AI Assist button and Enter activates it", %{
      conn: conn,
      source: source
    } do
      parent = self()
      stub(AnthropicClient, :configured?, fn -> true end)

      stub(AnthropicClient, :generate, fn prompt ->
        send(parent, {:anthropic_prompt, prompt})
        {:error, :unavailable}
      end)

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "warning"}}")
      |> wait_for_selector(".monaco-editor textarea.inputarea")
      |> press(".monaco-editor textarea.inputarea", "Tab")
      |> wait_for_selector("#ai-search-button:focus")
      |> press("#ai-search-button", "Enter")

      assert_receive {:anthropic_prompt, prompt}, 5_000
      assert prompt =~ "Natural-language request:\nwarning"
    end

    test "loads the remaining previous page of search results", %{
      conn: conn,
      source: source,
      user: user
    } do
      pagination_message = "featuresearchpagination#{System.unique_integer([:positive])}"

      log_events =
        for index <- 1..105 do
          build(:log_event, source: source, message: "#{pagination_message}-#{index}")
        end

      assert {:ok, 105} = Backends.ingest_logs(log_events, source)
      assert :ok = TestUtils.wait_for_postgres_events(source, user, pagination_message, 105)

      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(
        ~p"/sources/#{source.id}/search?#{%{querystring: ~s|event_message:~\"^#{pagination_message}-\"|, tailing?: false}}"
      )
      |> assert_has("#logs-list li[data-event-id]", count: 100)
      |> click("#load-more-events-top")
      |> assert_has("#logs-list li[data-event-id]", count: 105)
      |> refute_has("#load-more-events-top")
    end

    test "shows a missing field error from the search page", %{conn: conn, source: source} do
      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "s:nonexistent"}}")
      |> wait_for_selector(".message .alert", state: "attached")
      |> assert_has(".message .alert p", text: "nonexistent")
      |> assert_has(".message .alert p", text: "does not exist")
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

    test "clicking a rendered chart bar narrows the search datetime", %{
      conn: conn,
      source: source,
      user: user,
      matching_message: matching_message
    } do
      bar_selector = ~s|.recharts-bar-rectangle [height]:not([height="0"])|
      chart_selector = ".recharts-wrapper"

      assert :ok = TestUtils.wait_for_postgres_events(source, user, matching_message, 1)

      conn =
        conn
        |> visit(~p"/auth/login/single_tenant")
        |> assert_path(~p"/dashboard")
        |> visit(~p"/sources/#{source.id}/search")
        |> wait_for_selector(bar_selector)

      conn
      |> unwrap(fn %{frame_id: frame_id} ->
        {:ok, %{"x" => x, "y" => y}} =
          Frame.evaluate(frame_id,
            expression: """
            ({ barSelector, chartSelector }) => {
              const bar = document.querySelector(barSelector).getBoundingClientRect()
              const chart = document.querySelector(chartSelector).getBoundingClientRect()

              return {
                x: bar.left - chart.left + bar.width / 2,
                y: bar.top - chart.top + bar.height / 2
              }
            }
            """,
            is_function: true,
            arg: %{barSelector: bar_selector, chartSelector: chart_selector},
            timeout: 5_000
          )

        {:ok, _} =
          Frame.click(frame_id,
            selector: chart_selector,
            position: %{x: x, y: y},
            timeout: 5_000
          )
      end)

      querystring = wait_for_editor_querystring(conn, "..")

      assert querystring =~ ~r/t:\S+\.\.\S+/
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

  defp fill_editor(conn, value) do
    conn
    |> unwrap(fn %{frame_id: frame_id} ->
      Frame.fill(frame_id,
        selector: ".monaco-editor textarea.inputarea",
        value: value,
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
