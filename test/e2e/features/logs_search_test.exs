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
      source = insert(:source, user: user, suggested_keys: "event_message")

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
      |> assert_has("#logs-list-container", text: matching_message, timeout: 10_000)
      |> refute_has("#logs-list-container", text: non_matching_message, timeout: 10_000)
    end

    test "cancelling the datepicker resumes tailing", %{
      conn: conn,
      source: source
    } do
      conn
      |> visit(~p"/auth/login/single_tenant")
      |> assert_path(~p"/dashboard")
      |> visit(~p"/sources/#{source.id}/search")
      |> assert_has(".live-pause", text: "Live", timeout: 10_000)
      |> open_datepicker()
      |> click(".daterangepicker .cancelBtn")
      |> assert_has(".live-pause", text: "Pause", timeout: 10_000)
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
        |> open_datepicker()
        |> click(".daterangepicker .ranges li", "Last 15 Minutes")
        |> assert_has(".live-pause", text: "Live", timeout: 10_000)

      querystring =
        wait_for_editor_querystring(conn, fn querystring ->
          String.contains?(querystring, "t:last@15")
        end)

      assert querystring =~ "t:last@15"
    end
  end

  defp current_editor_querystring(conn) do
    ref = make_ref()

    conn
    |> unwrap(fn %{frame_id: frame_id} ->
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

  defp open_datepicker(conn, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_open_datepicker(conn, deadline)
  end

  defp wait_for_editor_querystring(conn, predicate, timeout_ms \\ 10_000) do
    deadline = System.monotonic_time(:millisecond) + timeout_ms
    do_wait_for_editor_querystring(conn, predicate, deadline)
  end

  defp do_open_datepicker(conn, deadline) do
    conn = click(conn, "#daterangepicker")

    case wait_for_selector(conn, ".daterangepicker", 500, state: "attached") do
      :ok ->
        conn

      :error ->
        if System.monotonic_time(:millisecond) < deadline do
          Process.sleep(100)
          do_open_datepicker(conn, deadline)
        else
          flunk("Timed out waiting for the datepicker to open")
        end
    end
  end

  defp wait_for_selector(conn, selector, timeout_ms, opts) do
    ref = make_ref()

    conn
    |> unwrap(fn %{frame_id: frame_id} ->
      result =
        case Frame.wait_for_selector(
               frame_id,
               Keyword.merge(opts, selector: selector, timeout: timeout_ms)
             ) do
          {:ok, _} -> :ok
          {:error, _} -> :error
        end

      send(self(), {ref, result})
    end)

    assert_receive {^ref, result}
    result
  end

  defp do_wait_for_editor_querystring(conn, predicate, deadline) do
    querystring = current_editor_querystring(conn)

    cond do
      predicate.(querystring) ->
        querystring

      System.monotonic_time(:millisecond) < deadline ->
        Process.sleep(100)
        do_wait_for_editor_querystring(conn, predicate, deadline)

      true ->
        flunk(
          "Timed out waiting for editor querystring update, last querystring: #{inspect(querystring)}"
        )
    end
  end
end
