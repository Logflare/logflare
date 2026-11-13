defmodule Logflare.QA.Browser do
  @moduledoc """
  Drives the Logflare UI in Chromium through `PlaywrightEx` and saves captures with
  plain-English expectations.

  A capture writes `<name>.png` and `<name>.json` to `Logflare.QA.Config.output_dir/0`.
  The JSON holds one to three claims about the picture. They are not executed: the
  reviewer reads the PNG and confirms or refutes each one. Assert the DOM state with
  `wait_for/3` before each capture, so a capture never shows a page that has not loaded.

  Every page records console errors and same-origin static assets that fail to load.
  `problems/1` returns them, and a check that finds any fails.
  """

  alias Logflare.QA.Config
  alias PlaywrightEx.Browser
  alias PlaywrightEx.BrowserContext
  alias PlaywrightEx.Frame
  alias PlaywrightEx.Page

  @timeout 30_000
  @max_expectations 3

  @problem_recorder """
  window.__qaProblems = [];
  window.addEventListener("error", (e) => {
    const target = e.target;
    if (target && (target.src || target.href)) window.__qaProblems.push(`failed to load ${target.src || target.href}`);
    else window.__qaProblems.push(`page error: ${e.message}`);
  }, true);
  const consoleError = console.error;
  console.error = (...args) => { window.__qaProblems.push(`console error: ${args.join(" ")}`); consoleError(...args); };
  """

  @type page :: %{page: String.t(), frame: String.t()}

  @spec start!() :: String.t()
  def start! do
    {:ok, _} =
      PlaywrightEx.Supervisor.start_link(
        timeout: @timeout,
        executable: Path.join(File.cwd!(), "assets/node_modules/playwright/cli.js")
      )

    launch_opts =
      case System.get_env("PLAYWRIGHT_CHROMIUM_PATH") do
        nil -> [timeout: @timeout]
        path -> [timeout: @timeout, executable_path: path]
      end

    {:ok, browser} = PlaywrightEx.launch_browser(:chromium, launch_opts)
    browser.guid
  end

  @spec new_page(String.t()) :: page()
  def new_page(browser) do
    {:ok, context} =
      Browser.new_context(browser,
        timeout: @timeout,
        base_url: Config.url(),
        viewport: %{width: 1280, height: 800}
      )

    {:ok, _} =
      BrowserContext.add_init_script(context.guid, timeout: @timeout, source: @problem_recorder)

    {:ok, page} = BrowserContext.new_page(context.guid, timeout: @timeout)
    %{page: page.guid, frame: page.main_frame.guid}
  end

  @spec goto(page(), String.t()) :: page()
  def goto(%{frame: frame} = page, path) do
    {:ok, _} = Frame.goto(frame, timeout: @timeout, url: Config.url() <> path)
    page
  end

  @doc "Waits until `selector` is visible. Raises when it is not visible within the timeout."
  @spec wait_for(page(), String.t(), keyword()) :: page()
  def wait_for(%{frame: frame} = page, selector, opts \\ []) do
    {:ok, _} =
      Frame.wait_for_selector(frame,
        selector: selector,
        state: Keyword.get(opts, :state, "visible"),
        timeout: Keyword.get(opts, :timeout, @timeout)
      )

    page
  end

  @spec click(page(), String.t()) :: page()
  def click(%{frame: frame} = page, selector) do
    {:ok, _} = Frame.click(frame, selector: selector, timeout: @timeout)
    page
  end

  @spec fill(page(), String.t(), String.t()) :: page()
  def fill(%{frame: frame} = page, selector, value) do
    {:ok, _} = Frame.fill(frame, selector: selector, value: value, timeout: @timeout)
    page
  end

  @doc "Evaluates a JavaScript expression in the page and returns its value."
  @spec eval(page(), String.t()) :: term()
  def eval(%{frame: frame}, expression) do
    {:ok, value} =
      Frame.evaluate(frame, expression: expression, is_function: false, timeout: @timeout)

    value
  end

  @spec url(page()) :: String.t()
  def url(page), do: eval(page, "location.href")

  @spec problems(page()) :: [String.t()]
  def problems(page), do: eval(page, "window.__qaProblems || []")

  @doc """
  Saves a capture and its expectations. Options: `clip: selector` captures only that
  element's area of the viewport.
  """
  @spec capture(page(), String.t(), [String.t()], keyword()) :: Path.t()
  def capture(%{page: page_id} = page, name, expectations, opts \\ []) do
    count = length(expectations)

    if count == 0 or count > @max_expectations do
      raise ArgumentError,
            "capture #{name} needs 1 to #{@max_expectations} expectations, got #{count}. " <>
              "A capture that needs more claims shows more than one thing: take a second capture."
    end

    screenshot_opts =
      case Keyword.fetch(opts, :clip) do
        {:ok, selector} -> [timeout: @timeout, clip: bounding_box(page, selector)]
        :error -> [timeout: @timeout]
      end

    {:ok, png} = Page.screenshot(page_id, screenshot_opts)

    dir = Config.output_dir()
    File.mkdir_p!(dir)
    png_path = Path.join(dir, "#{name}.png")
    File.write!(png_path, Base.decode64!(png))

    manifest = %{
      name: name,
      url: url(page),
      captured_at: DateTime.utc_now(),
      expectations: expectations
    }

    File.write!(Path.join(dir, "#{name}.json"), Jason.encode!(manifest, pretty: true))
    png_path
  end

  defp bounding_box(page, selector) do
    eval(page, """
    (() => {
      const r = document.querySelector(#{Jason.encode!(selector)}).getBoundingClientRect();
      return {x: r.x, y: r.y, width: r.width, height: r.height};
    })()
    """)
  end
end
