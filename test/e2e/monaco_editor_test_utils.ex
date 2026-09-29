defmodule LogflareWeb.MonacoEditorTestUtils do
  @moduledoc false

  alias PhoenixTest.Playwright
  alias PlaywrightEx.Frame

  @editor_input_selector ".monaco-editor textarea.inputarea"
  @editor_selector ".monaco-editor"

  @spec wait_for_monaco_editor(Playwright.t()) :: Playwright.t()
  def wait_for_monaco_editor(conn) do
    conn
    |> Playwright.unwrap(fn %{frame_id: frame_id} ->
      {:ok, _} =
        Frame.wait_for_selector(frame_id,
          selector: @editor_input_selector,
          state: "attached",
          timeout: 40_000
        )
    end)
  end

  @spec replace_monaco_text(Playwright.t(), String.t()) :: Playwright.t()
  def replace_monaco_text(conn, text) do
    conn =
      conn
      |> Playwright.click(@editor_selector)
      |> Playwright.unwrap(fn %{frame_id: frame_id} ->
        {:ok, _} = Frame.focus(frame_id, selector: @editor_input_selector, timeout: 5_000)

        {:ok, _} =
          Frame.press(frame_id,
            selector: @editor_input_selector,
            key: "Control+A",
            timeout: 5_000
          )

        {:ok, _} =
          Frame.type(frame_id, selector: @editor_input_selector, text: text, timeout: 5_000)
      end)

    wait_for_monaco_text(conn, text)
  end

  @spec fill_input(Playwright.t(), String.t(), String.t()) :: Playwright.t()
  def fill_input(conn, selector, value) do
    conn
    |> Playwright.unwrap(fn %{frame_id: frame_id} ->
      {:ok, _} =
        Frame.fill(frame_id,
          selector: selector,
          value: value,
          timeout: 5_000
        )
    end)
  end

  @spec wait_for_input_value(Playwright.t(), String.t(), String.t(), non_neg_integer()) ::
          Playwright.t()
  def wait_for_input_value(conn, selector, expected_value, timeout_ms \\ 10_000) do
    conn
    |> Playwright.unwrap(fn %{frame_id: frame_id} ->
      {:ok, _} =
        Frame.wait_for_function(frame_id,
          expression: """
          ({ selector, expectedValue }) => {
            return Array.from(document.querySelectorAll(selector))
              .some((field) => field.value === expectedValue)
          }
          """,
          is_function: true,
          arg: %{selector: selector, expectedValue: expected_value},
          timeout: timeout_ms
        )
    end)
  end

  @spec wait_for_monaco_text(Playwright.t(), String.t(), non_neg_integer()) :: Playwright.t()
  def wait_for_monaco_text(conn, expected_text, timeout_ms \\ 10_000) do
    conn
    |> Playwright.unwrap(fn %{frame_id: frame_id} ->
      {:ok, _} =
        Frame.wait_for_function(frame_id,
          expression: """
          ({ expectedText }) => {
            const editorText =
              window.monaco?.editor?.getModels?.()[0]?.getValue?.() ||
              document.querySelector(".monaco-editor textarea.inputarea")?.value ||
              Array.from(document.querySelectorAll(".monaco-editor .view-line"))
                .map((line) => line.textContent)
                .join("\\n")

            return editorText.includes(expectedText)
          }
          """,
          is_function: true,
          arg: %{expectedText: expected_text},
          timeout: timeout_ms
        )
    end)
  end
end
