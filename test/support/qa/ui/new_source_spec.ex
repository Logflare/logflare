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

    form =
      Browser.capture(page, "new-source-form", [
        "The subhead reads ~/logs/new, and an Add source button sits below the form."
      ])

    page
    |> Browser.click(~s|button:has-text("Add source")|)
    |> Browser.wait_for("text=Source created!")
    |> Browser.wait_for("text=#{name}")
    |> Browser.assert_no_problems!()

    [
      form,
      Browser.capture(page, "new-source-created", [
        "A Source created! flash shows, and the subhead reads ~/logs/#{name}."
      ])
    ]
  end
end
