defmodule Logflare.QA.Ingest.SearchSpec do
  @moduledoc """
  Opens the QA source's search page filtered by a run id, asserts that exactly that
  run's events are listed, and captures the query bar and the results.
  """

  alias Logflare.QA.Browser

  @rows "#logs-list > li[data-event-id]"

  @spec run(String.t(), pos_integer(), String.t(), [String.t()]) :: [Path.t()]
  def run(browser, source_id, run_id, messages) do
    # Sign in first: the single-tenant sign-in redirect drops the query string of the first URL.
    page =
      browser
      |> Browser.new_page()
      |> Browser.goto("/dashboard")
      |> Browser.goto("/sources/#{source_id}/search?querystring=#{URI.encode_www_form(run_id)}")

    url = Browser.url(page)
    url =~ "querystring=#{run_id}" || raise "the search URL lost its query: #{url}"

    for message <- messages, do: Browser.wait_for(page, ~s|#{@rows}:has-text("#{message}")|)

    expected_rows = length(messages)
    rows = Browser.eval(page, "document.querySelectorAll(#{Jason.encode!(@rows)}).length")
    rows == expected_rows || raise "expected #{expected_rows} result rows, found #{rows}"

    Browser.wait_for(page, ~s|.monaco-editor .view-lines:has-text("#{run_id}")|)
    problems = Browser.problems(page)
    problems == [] || raise "page problems: #{inspect(problems)}"

    query =
      Browser.capture(page, "ingest-search-01-query", [
        "The query editor above the Search button contains #{run_id}.",
        "The subhead reads ~/logs/qa_ingest_main/search."
      ])

    # The page scrolls to the newest event while tailing, which puts the first rows under the sticky header.
    # Scrolling to the top shows a "Load more" button above the list, so let the layout settle.
    Browser.eval(page, "window.scrollTo(0, 0)")
    Browser.eval(page, "new Promise((resolve) => setTimeout(resolve, 500))")

    results =
      Browser.capture(
        page,
        "ingest-search-02-results",
        [
          "The list has #{expected_rows} rows, and each message ends with #{run_id}.",
          "Rows come from http_token, http_name, websocket and grpc, each with error, warn and info.",
          "Each row starts with a green timestamp and ends with a view context link."
        ],
        clip: "#logs-list"
      )

    [query, results]
  end
end
