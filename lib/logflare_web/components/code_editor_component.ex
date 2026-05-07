defmodule LogflareWeb.MonacoEditorComponentNew do
  use Phoenix.Component

  attr :field, Phoenix.HTML.FormField, required: true
  attr :id, :string, default: "lf-monaco"
  attr :language, :string, default: "sql"
  attr :opts, :map, default: %{}
  attr :completions, :list, default: []
  attr :schema_fields, :map, default: %{}
  attr :suggested_searches, :list, default: []
  attr :debounce, :string, default: nil
  attr :class, :string, default: "tw-mb-12"
  attr :editor_class, :string, default: "tw-w-full"

  def code_editor(assigns) do
    assigns =
      assigns
      |> assign(:completions_json, Jason.encode!(assigns.completions))
      |> assign(:opts_json, Jason.encode!(assigns.opts))
      |> assign(:schema_fields_json, Jason.encode!(assigns.schema_fields))
      |> assign(:suggested_searches_json, Jason.encode!(assigns.suggested_searches))

    ~H"""
    <div class={@class} id={@id} phx-hook="MonacoHook" data-language={@language} data-options={@opts_json} data-completions={@completions_json} data-schema-fields-json={@schema_fields_json} data-suggested-searches-json={@suggested_searches_json}>
      <input id={"#{@id}-input"} type="hidden" name={@field.name} value={@field.value} phx-debounce={@debounce} data-editor-input />
      <div id={"#{@id}-editor"} phx-update="ignore" class={@editor_class}>
        <div id={"#{@id}-editor-container"} data-editor-container></div>
      </div>
    </div>
    """
  end

  def lql_editor_opts do
    %{
      "language" => "lql",
      "lineNumbers" => "off",
      "glyphMargin" => false,
      "folding" => false,
      "lineDecorationsWidth" => 8,
      "lineNumbersMinChars" => 0,
      "wordWrap" => "off",
      "scrollbar" => %{
        "horizontal" => "hidden",
        "vertical" => "hidden",
        "handleMouseWheel" => false
      },
      "overviewRulerLanes" => 0,
      "overviewRulerBorder" => false,
      "hideCursorInOverviewRuler" => true,
      "contextmenu" => false,
      "fixedOverflowWidgets" => true,
      "suggest" => %{"enabled" => true, "showWords" => false},
      "parameterHints" => %{"enabled" => false},
      "quickSuggestions" => true,
      "matchBrackets" => "never",
      "fontSize" => 14,
      "tabIndex" => 0,
      "fontFamily" =>
        "SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace",
      "padding" => %{"top" => 5, "bottom" => 5},
      "automaticLayout" => true,
      "minHeight" => 32
    }
  end
end
