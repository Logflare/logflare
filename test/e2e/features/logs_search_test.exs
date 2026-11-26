defmodule E2e.Features.LogsSearchTest do
  use Logflare.FeatureCase, async: false

  alias Ecto.Changeset
  alias Logflare.Backends
  alias Logflare.NaturalLanguageLql.AnthropicClient
  alias Logflare.Repo
  alias Logflare.SingleTenant

  import Fluffy.Playwright,
    only: [bounding_box: 2, dispatch_event: 4, scroll_into_view_if_needed: 2]

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
      session: session,
      source: source,
      matching_message: matching_message,
      non_matching_message: non_matching_message
    } do
      session
      |> log_in()
      |> visit(
        ~p"/sources/#{source.id}/search?#{%{querystring: "event_message:#{matching_message}"}}"
      )
      |> assert(contain_text(by_css("#logs-list-container"), matching_message))
      |> refute(contain_text(by_css("#logs-list-container"), non_matching_message))
    end

    test "filters timezone options, remembers a selection, and resets to UTC", %{
      session: session,
      source: source,
      user: user
    } do
      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search?#{%{tz: "Etc/UTC"}}")
      |> assert(value(timezone_combobox(), "Etc/UTC (+00:00)"))
      |> fill(timezone_combobox(), "Brisbane")
      |> assert(visible(by_role(:option, name: "Australia/Brisbane (+10:00)")))
      |> refute(visible(by_role(:option, name: "Europe/London")))
      |> click(by_role(:option, name: "Australia/Brisbane (+10:00)"))
      |> assert_search_timezone("Australia/Brisbane")
      |> assert(value(timezone_combobox(), "Australia/Brisbane (+10:00)"))
      |> check(by_label("Remember"))
      |> assert(visible(by_text("Timezone preference saved")))
      |> refute(visible(by_label("Remember")))
      |> click(by_role(:button, name: "UTC", exact: true))
      |> assert_search_timezone("Etc/UTC")
      |> assert(value(timezone_combobox(), "Etc/UTC (+00:00)"))

      assert Repo.reload!(user).preferences.timezone == "Australia/Brisbane"
    end

    test "Cmd/Ctrl+Enter submits the editor value to AI Assist", %{
      session: session,
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

      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "warning"}}")
      |> assert(visible(ai_assist_button()))
      |> fill(by_label("metadata.level"), " error ")
      |> press(editor(), "ControlOrMeta+Enter")

      assert_receive {:anthropic_prompt, prompt}, 5_000
      assert prompt =~ "<request>\nwarning"
      assert prompt =~ "\n</request>"

      session
      |> assert_editor_querystring(~s|~"(?i)error"|)
      |> assert_editor_querystring("m.level:error")
    end

    test "Enter submits a regular search from the editor", %{
      session: session,
      source: source,
      non_matching_message: non_matching_message
    } do
      stub(AnthropicClient, :configured?, fn -> true end)
      reject(AnthropicClient, :generate, 1)

      query = "event_message:#{non_matching_message}"

      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "warning"}}")
      |> fill(editor(), query)
      |> press(editor(), "Enter")
      |> assert_editor_querystring(non_matching_message)
    end

    test "Tab focuses the AI Assist button and Enter activates it", %{
      session: session,
      source: source
    } do
      parent = self()
      stub(AnthropicClient, :configured?, fn -> true end)

      stub(AnthropicClient, :generate, fn prompt ->
        send(parent, {:anthropic_prompt, prompt})
        {:error, :unavailable}
      end)

      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "warning"}}")
      |> press(editor(), "Tab")
      |> assert(focused(ai_assist_button()))
      |> press(ai_assist_button(), "Enter")

      assert_receive {:anthropic_prompt, prompt}, 5_000
      assert prompt =~ "<request>\nwarning"
      assert prompt =~ "\n</request>"
    end

    test "loads the remaining previous page of search results", %{
      session: session,
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

      session
      |> log_in()
      |> visit(
        ~p"/sources/#{source.id}/search?#{%{querystring: ~s|event_message:~\"^#{pagination_message}-\"|, tailing?: false}}"
      )
      |> assert(count(log_rows(), 100))
      |> click(load_more_top_button())
      |> assert(count(log_rows(), 105))
      |> assert(enabled(load_more_top_button()))
    end

    @scroll_anchor_tail "at Module.handleRequest (/srv/app/lib/handler.js:142:19) -> at Router.dispatch (/srv/app/node_modules/router/index.js:88:7) -> at Layer.handle (/srv/app/node_modules/router/layer.js:95:5)"

    test "an older page keeps the row the reader is on in place", %{
      session: session,
      source: source,
      user: user
    } do
      prefix = "featurescrollanchor#{System.unique_integer([:positive])}"

      log_events =
        for index <- 1..150 do
          build(:log_event,
            source: source,
            message: "#{prefix}-#{index} request failed with status 503 #{@scroll_anchor_tail}"
          )
        end

      assert {:ok, 150} = Backends.ingest_logs(log_events, source)
      assert :ok = TestUtils.wait_for_postgres_events(source, user, prefix, 150)

      session
      |> log_in()
      |> visit(
        ~p"/sources/#{source.id}/search?#{%{querystring: ~s|event_message:~\"^#{prefix}-\"|, tailing?: false}}"
      )
      |> assert(count(log_rows(), 100))
      |> assert(enabled(load_more_top_button()))
      |> scroll_into_view_if_needed(load_more_top_button())

      # The oldest row sits right below the button, so it is the first one the reader sees.
      event_id = get_attribute(session, first(log_rows()), "data-event-id")
      anchor = by_css(~s|#logs-list li[data-event-id="#{event_id}"]|)
      before = bounding_box(session, anchor)

      assert before.y > 0, "no row was visible to anchor on"

      session
      |> click(load_more_top_button())
      |> assert(count(log_rows(), 150))
      |> assert(
        poll(
          fn session -> round(bounding_box(session, anchor).y - before.y) end,
          fn drift ->
            assert abs(drift) <= 4,
                   "the reader's row moved #{drift}px. An older page must not move the viewport."
          end
        )
      )
    end

    test "shows a missing field error from the search page", %{session: session, source: source} do
      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search?#{%{querystring: "s:nonexistent"}}")
      |> assert(contain_text(by_css(".message .alert p"), "nonexistent"))
      |> assert(contain_text(by_css(".message .alert p"), "does not exist"))
    end

    test "cancelling the datepicker resumes tailing", %{
      session: session,
      source: source
    } do
      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search")
      |> assert(contain_text(live_pause_button(), "Pause"))
      |> click(by_css("#daterangepicker"))
      |> click_date_range_cancel()
      |> assert(contain_text(live_pause_button(), "Pause"))
    end

    test "applying a preset date range updates the search query", %{
      session: session,
      source: source
    } do
      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search")
      |> click(by_text("DateTime", exact: true))
      |> click_date_range_preset("Last 15 Minutes")
      |> assert(contain_text(live_pause_button(), "Live"))
      |> assert_editor_querystring("t:last@15")
    end

    test "changing chart period updates the search query", %{
      session: session,
      source: source
    } do
      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search")
      |> assert(visible(by_css("#source-logs-search-list")))
      |> assert_editor_querystring(~r/./)
      |> select_option(by_css("#search_chart_period"), "hour")
      |> assert_editor_querystring("c:group_by(t::hour)")
    end

    test "clicking a rendered chart bar narrows the search datetime", %{
      session: session,
      source: source,
      user: user,
      matching_message: matching_message
    } do
      bar = by_css(~s|.recharts-bar-rectangle [height]:not([height="0"])|)

      assert :ok = TestUtils.wait_for_postgres_events(source, user, matching_message, 1)

      session
      |> log_in()
      |> visit(~p"/sources/#{source.id}/search")
      # The bars have `pointer-events="none"`; the chart surface under them takes the click.
      |> click(first(bar), force: true)
      |> assert_editor_querystring(~r/t:\S+\.\.\S+/)
    end
  end

  @spec assert_search_timezone(Fluffy.Session.t(), String.t()) :: Fluffy.Session.t()
  defp assert_search_timezone(session, timezone) do
    session
    |> assert(page_url(&(URI.decode_query(&1.query || "")["tz"] == timezone)))
    |> assert(value(by_css("#results-actions_search_timezone"), timezone))
  end

  defp click_date_range_preset(session, preset) do
    trigger_click_event(session, ~s|.daterangepicker .ranges li[data-range-key="#{preset}"]|)
  end

  defp click_date_range_cancel(session) do
    trigger_click_event(session, ".daterangepicker .cancelBtn")
  end

  defp trigger_click_event(session, selector) do
    dispatch_event(session, by_css(selector), "click", %{bubbles: true, cancelable: true})
  end

  defp assert_editor_querystring(session, expected) do
    expected = if is_binary(expected), do: ~r/#{Regex.escape(expected)}/, else: expected

    assert(session, attribute(by_css("#lql-editor-hook"), "data-querystring", expected))
  end

  defp timezone_combobox, do: by_role(:combobox, name: "Display timezone")

  defp editor, do: by_css(".monaco-editor textarea.inputarea")

  defp ai_assist_button, do: by_role(:button, name: "AI search assistant")

  defp log_rows, do: by_css("#logs-list li[data-event-id]")

  defp load_more_top_button, do: by_css("#load-more-events-top")

  defp live_pause_button, do: by_css(".live-pause")
end
