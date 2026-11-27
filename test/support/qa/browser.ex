defmodule Logflare.QA.Browser do
  @moduledoc """
  Drives the Logflare UI in Chromium through `PlaywrightEx` and saves captures.

  A capture writes `<name>.png` and `<name>.json` to `tmp/qa/`. The JSON lists one to
  three plain-English claims about the picture, for the reviewer to confirm or refute
  against the PNG. Assert the DOM state with `wait_for/3` before each capture.
  """

  alias Logflare.QA.Config
  alias PlaywrightEx.Browser
  alias PlaywrightEx.BrowserContext
  alias PlaywrightEx.Frame
  alias PlaywrightEx.Page

  @timeout 30_000

  @problem_recorder """
  window.__qaProblems = [];
  window.addEventListener("error", (e) => {
    const src = e.target && (e.target.src || e.target.href);
    window.__qaProblems.push(src ? `failed to load ${src}` : `page error: ${e.message}`);
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

    executable = System.get_env("PLAYWRIGHT_CHROMIUM_PATH")
    launch_opts = if executable, do: [executable_path: executable], else: []
    {:ok, browser} = PlaywrightEx.launch_browser(:chromium, [timeout: @timeout] ++ launch_opts)
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
  def goto(page, path), do: frame_call(page, :goto, url: Config.url() <> path)

  @doc "Waits until `selector` is visible, or raises."
  @spec wait_for(page(), String.t()) :: page()
  def wait_for(page, selector),
    do: frame_call(page, :wait_for_selector, selector: selector, state: "visible")

  @spec click(page(), String.t()) :: page()
  def click(page, selector), do: frame_call(page, :click, selector: selector)

  @spec fill(page(), String.t(), String.t()) :: page()
  def fill(page, selector, value), do: frame_call(page, :fill, selector: selector, value: value)

  @spec eval(page(), String.t()) :: term()
  def eval(%{frame: frame}, expression) do
    {:ok, value} =
      Frame.evaluate(frame, expression: expression, is_function: false, timeout: @timeout)

    value
  end

  @doc "Raises on console errors or same-origin assets that failed to load."
  @spec assert_no_problems!(page()) :: page()
  def assert_no_problems!(page) do
    problems = eval(page, "window.__qaProblems || []")
    problems == [] || raise "page problems: #{inspect(problems)}"
    page
  end

  @doc "Saves a capture with its expectations. `clip: selector` captures only that element."
  @spec capture(page(), String.t(), [String.t()], keyword()) :: Path.t()
  def capture(%{page: page_id} = page, name, expectations, opts \\ [])
      when length(expectations) in 1..3 do
    clip = if selector = opts[:clip], do: [clip: bounding_box(page, selector)], else: []
    {:ok, png} = Page.screenshot(page_id, [timeout: @timeout] ++ clip)

    File.mkdir_p!(Config.output_dir())
    path = Path.join(Config.output_dir(), "#{name}.png")
    File.write!(path, Base.decode64!(png))
    manifest = %{name: name, url: eval(page, "location.href"), expectations: expectations}

    File.write!(
      Path.join(Config.output_dir(), "#{name}.json"),
      Jason.encode!(manifest, pretty: true)
    )

    path
  end

  defp frame_call(%{frame: frame} = page, fun, opts) do
    {:ok, _} = apply(Frame, fun, [frame, [timeout: @timeout] ++ opts])
    page
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
