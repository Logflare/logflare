defmodule Mix.Tasks.Qa.Ui do
  @shortdoc "Runs the UI screenshot specs against a running Logflare server"
  @moduledoc """
  Drives the UI in Chromium and saves captures with plain-English expectations to
  `tmp/qa/`. Runs against `LOGFLARE_URL`: a release image or `mix qa.server`.

      mix qa.ui                 # all specs
      mix qa.ui dashboard       # one spec

  Prints `QA_CHECK PASS|FAIL` and `QA_CAPTURE <png>` lines, then `QA_RESULT`.
  Verify each capture against the expectations in its `.json` file.
  """
  use Mix.Task

  alias Logflare.QA.Browser
  alias Logflare.QA.Report
  alias Logflare.QA.UI.DashboardSpec
  alias Logflare.QA.UI.NewSourceSpec

  @specs [
    {"dashboard", "dashboard loads with styles, icons, and images", DashboardSpec},
    {"new-source", "creates a source from the dashboard", NewSourceSpec}
  ]

  @impl Mix.Task
  def run(names) do
    Mix.Task.run("compile")
    {:ok, _} = Application.ensure_all_started([:jason, :logger])

    selected =
      if names == [], do: @specs, else: Enum.filter(@specs, fn {name, _, _} -> name in names end)

    selected == [] &&
      Mix.raise(
        "No spec matches #{inspect(names)}. Specs: #{Enum.map_join(@specs, ", ", &elem(&1, 0))}"
      )

    browser = Browser.start!()

    selected
    |> Enum.map(fn {_, label, spec} -> Report.spec(label, fn -> spec.run(browser) end) end)
    |> Report.finish!("ui")
  end
end
