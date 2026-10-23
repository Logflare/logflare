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

    url = Browser.url(page)
    url =~ "/dashboard" || raise "expected a redirect to /dashboard, got #{url}"

    styled_sheets =
      Browser.eval(page, """
      [...document.styleSheets].filter(
        (s) => s.href && new URL(s.href).origin === location.origin && s.cssRules.length > 0
      ).length
      """)

    styled_sheets > 0 || raise "no same-origin stylesheet loaded"

    broken_images =
      Browser.eval(
        page,
        "[...document.images].filter((i) => i.complete && i.naturalWidth === 0).map((i) => i.src)"
      )

    broken_images == [] || raise "broken images: #{inspect(broken_images)}"
    problems = Browser.problems(page)
    problems == [] || raise "page problems: #{inspect(problems)}"

    [
      Browser.capture(page, "dashboard-01-page", [
        "The top bar is green and shows the Logflare logo and version on the left.",
        "The content area has a dark background with Members, sources, and Integrations columns.",
        "Run a query and New source show as blue buttons, not as plain links."
      ]),
      Browser.capture(
        page,
        "dashboard-02-subhead-icons",
        [
          "Each subhead link (ingest API key, access tokens, billing, help) has an icon to its left.",
          "The icons are glyphs, not empty boxes or missing-character squares."
        ],
        clip: ".subhead"
      )
    ]
  end
end
