defmodule Logflare.QA.Ingest.SearchSpec do
  @moduledoc """
  Opens the QA source's search page filtered by a run id, asserts that exactly that
  run's events are listed and the query editor shows the run id, and captures the list.

  It signs in at `/dashboard` first, because the single-tenant sign-in redirect drops the
  query string. Tailing scrolls the newest event under the sticky header, so it scrolls
  back to the top and lets the "Load more" button settle before the capture.
  """

  alias Logflare.QA.Browser

  @rows "#logs-list > li[data-event-id]"

  @spec run(String.t(), pos_integer(), String.t(), [String.t()]) :: [Path.t()]
  def run(browser, source_id, run_id, messages) do
    page =
      browser
      |> Browser.new_page()
      |> sign_in()
      |> Browser.goto("/sources/#{source_id}/search?querystring=#{URI.encode_www_form(run_id)}")
      |> Browser.wait_for(~s|.monaco-editor .view-lines:has-text("#{run_id}")|)

    for message <- messages, do: Browser.wait_for(page, ~s|#{@rows}:has-text("#{message}")|)

    rows = Browser.eval(page, "document.querySelectorAll(#{Jason.encode!(@rows)}).length")
    rows == length(messages) || raise "expected #{length(messages)} result rows, found #{rows}"

    scroll_to_top(page)

    capture =
      Browser.capture(
        page,
        "ingest-search-results",
        [
          "The list has #{rows} rows, and each message ends with #{run_id}.",
          "Rows come from http_token, http_name, websocket and grpc, each with error, warn and info.",
          "Each row starts with a green timestamp and ends with a view context link."
        ],
        clip: "#logs-list"
      )

    Browser.assert_no_problems!(page)
    [capture]
  end

  defp sign_in(page), do: Browser.goto(page, "/dashboard")

  defp scroll_to_top(page) do
    Browser.eval(page, "window.scrollTo(0, 0)")
    Browser.eval(page, "new Promise((resolve) => setTimeout(resolve, 500))")
  end
end
