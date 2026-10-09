defmodule Logflare.QA.UI.NewSourceSpec do
  @moduledoc "Creates a source from the dashboard."

  alias Logflare.QA.Browser

  @spec run(String.t()) :: [Path.t()]
  def run(browser) do
    name = "ui-qa.#{System.os_time(:millisecond)}"

    page =
      browser
      |> Browser.new_page()
      |> Browser.goto("/dashboard")
      |> Browser.click("text=New source")
      |> Browser.fill(~s|input[placeholder="YourApp.SourceName"]|, name)

    url = Browser.url(page)
    url =~ "/sources/new" || raise "expected /sources/new, got #{url}"

    form =
      Browser.capture(page, "new-source-01-form", [
        "The subhead reads ~/logs/new.",
        "The source name field shows the typed name, and an Add source button sits below the form."
      ])

    page
    |> Browser.click(~s|button:has-text("Add source")|)
    |> Browser.wait_for("text=Source created!")
    |> Browser.wait_for("text=#{name}")

    problems = Browser.problems(page)
    problems == [] || raise "page problems: #{inspect(problems)}"

    created =
      Browser.capture(page, "new-source-02-created", [
        "A success flash reading Source created! is shown.",
        "The new source name appears on the page."
      ])

    [form, created]
  end
end
