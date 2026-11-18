defmodule Logflare.QA.UI.DashboardSpec do
  @moduledoc "The dashboard loads with its styles, icons and images."

  alias Logflare.QA.Browser

  @spec run(String.t()) :: [Path.t()]
  def run(browser) do
    page =
      browser
      |> Browser.new_page()
      |> Browser.goto("/")
      |> Browser.wait_for("text=New source")

    url = Browser.eval(page, "location.href")
    url =~ "/dashboard" || raise "expected a redirect to /dashboard, got #{url}"

    Browser.eval(page, "[...document.styleSheets].some((s) => s.href && s.cssRules.length > 0)") ||
      raise "no stylesheet loaded"

    broken =
      Browser.eval(
        page,
        "[...document.images].filter((i) => i.complete && !i.naturalWidth).map((i) => i.src)"
      )

    broken == [] || raise "broken images: #{inspect(broken)}"

    capture =
      Browser.capture(page, "dashboard", [
        "The top bar is green and shows the Logflare logo and version on the left.",
        "Each subhead link (ingest API key, access tokens, billing, help) has a glyph icon to its left.",
        "Run a query and New source show as blue buttons, not as plain links."
      ])

    Browser.assert_no_problems!(page)
    [capture]
  end
end
