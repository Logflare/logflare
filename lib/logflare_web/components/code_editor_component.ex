defmodule LogflareWeb.MonacoEditorComponentNew do
  use Phoenix.Component

  @default_opts %{
    "automaticLayout" => true,
    "contextmenu" => false,
    "editContext" => false,
    "folding" => false,
    "fontFamily" => "JetBrains Mono, monospace",
    "fontSize" => 12,
    "formatOnPaste" => true,
    "formatOnType" => true,
    "glyphMargin" => false,
    "guides" => %{
      "indentation" => false
    },
    "hideCursorInOverviewRuler" => true,
    "lineNumbers" => "off",
    "lineNumbersMinChars" => 0,
    "minimap" => %{
      "enabled" => false
    },
    "occurrencesHighlight" => false,
    "padding" => %{
      "top" => 14,
      "bottom" => 14
    },
    "parameterHints" => true,
    "renderLineHighlight" => "none",
    "roundedSelection" => true,
    "scrollbar" => %{
      "vertical" => "auto",
      "horizontal" => "hidden",
      "verticalScrollbarSize" => 6,
      "alwaysConsumeMouseWheel" => false
    },
    "scrollBeyondLastLine" => false,
    "smoothScrolling" => true,
    "stickyScroll" => %{
      "enabled" => false
    },
    "suggestSelection" => "first",
    "tabCompletion" => "on",
    "tabIndex" => -1,
    "tabSize" => 2,
    "theme" => "default",
    "wordWrap" => "on"
  }

  attr :field, Phoenix.HTML.FormField, required: true
  attr :id, :string, default: "lf-monaco"
  attr :language, :string, default: "sql"
  attr :opts, :map, default: %{}
  attr :completions, :list, default: []
  attr :schema_fields, :map, default: %{}
  attr :suggested_searches, :list, default: []
  attr :class, :string, default: "tw-mb-12"
  attr :editor_class, :string, default: "tw-w-full"

  def code_editor(assigns) do
    opts =
      assigns.opts
      |> default_opts()

    assigns =
      assigns
      |> assign(:completions_json, Jason.encode!(assigns.completions))
      |> assign(:opts_json, Jason.encode!(opts))
      |> assign(:schema_fields_json, Jason.encode!(assigns.schema_fields))
      |> assign(:suggested_searches_json, Jason.encode!(assigns.suggested_searches))

    ~H"""
    <div class={@class} id={@id} phx-hook="MonacoHook" data-language={@language} data-options={@opts_json} data-completions={@completions_json} data-schema-fields-json={@schema_fields_json} data-suggested-searches-json={@suggested_searches_json}>
      <input type="hidden" name={@field.name} value={@field.value} phx-debounce="300" data-editor-input />
      <div id={[@id, "-editor"]} phx-update="ignore" class={@editor_class}>
        <div data-editor-container></div>
      </div>
    </div>
    """
  end

  @spec default_opts(map()) :: map()
  def default_opts(overrides \\ %{}) do
    DeepMerge.deep_merge(@default_opts, overrides)
  end

  @spec lql_editor_opts() :: map()
  def lql_editor_opts do
    %{
      "lineDecorationsWidth" => 8,
      "wordWrap" => "off",
      "scrollbar" => %{
        "vertical" => "hidden",
        "handleMouseWheel" => false
      },
      "overviewRulerLanes" => 0,
      "overviewRulerBorder" => false,
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
      "minHeight" => 32
    }
    |> default_opts()
  end
end
