defmodule LogflareWeb.MonacoEditorComponentNew do
  use Phoenix.Component

  attr :value, :string, default: ""
  attr :id, :string, default: "monaco-hook"
  attr :name, :string, default: "query"
  attr :completions, :list, default: []

  def code_editor(assigns) do
    assigns = assign(assigns, :completions_json, Jason.encode!(assigns.completions))

    ~H"""
    <div class="tw-mb-12 " id={@id} phx-hook="MonacoHook" data-completions={@completions_json}>
      <textarea name={@name} class="tw-hidden">{@value}</textarea>
      <div id={"#{@id}-editor"} phx-update="ignore" class="tw-w-full" data-editor-container></div>
    </div>
    """
  end
end
