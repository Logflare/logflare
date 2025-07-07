defmodule LogflareWeb.MonacoEditorComponent do
  use LogflareWeb, :live_component

  def mount(socket) do
    {:ok, assign(socket, parse_error_message: nil)}
  end

  def render(assigns) do
    assigns = assigns |> assign(editor_opts: editor_opts())

    ~H"""
    <div>
      <LiveMonacoEditor.code_editor value={@field.value} target={@myself} change="parse-query" path="query_string" id="query" opts={@editor_opts} />
      <%= hidden_input(@field.form, :query, value: @field.value) %>
      <%= error_tag(@field.form, :query) %>
      <.alert :if={@parse_error_message} variant="warning">
        <strong>SQL Parse error!</strong>
        <br />
        <span><%= @parse_error_message %></span>
      </.alert>
      <.completions_script completions={@completions} id={@field.id <> "_completions"} />
    </div>
    """
  end

  attr :completions, :list, required: true, examples: [["source_name", "logs"]]
  attr :id, :string, required: true

  def completions_script(assigns) do
    ~H"""
    <script phx-update="ignore" id={@id}>
      window.addEventListener("lme:editor_mounted", (event) => {
        const completions = <%= Jason.encode!(@completions) |> raw() %>

        if (completions.length == 0) { return; }

        const editor = event.detail.editor.standalone_code_editor;

        function createDependencyProposals(range) {
          return completions.map(function (name) {
            return {
              label: name,
              kind: monaco.languages.CompletionItemKind.Module,
              insertText: name,
              range: range,
            };
          });
        }

        monaco.languages.registerCompletionItemProvider("sql", {
          provideCompletionItems: function (model, position) {
            var word = model.getWordUntilPosition(position);
            var range = {
              startLineNumber: position.lineNumber,
              endLineNumber: position.lineNumber,
              startColumn: word.startColumn,
              endColumn: word.endColumn,
            };

            return {
              suggestions: createDependencyProposals(range),
            };
          },
        });
      });
    </script>
    """
  end

  def handle_event("parse-query", %{"value" => query}, socket) do
    %{endpoints: endpoints, alerts: alerts} = socket.assigns

    field = socket.assigns.field |> Map.put(:value, query)

    socket =
      case parse_query(query, endpoints, alerts) do
        :ok ->
          assign(socket, field: field, parse_error_message: nil)

        {:error, message} ->
          assign(socket, field: field, parse_error_message: message)
      end

    {:noreply, socket}
  end

  def handle_event("parse-query", %{"_target" => ["live_monaco_editor", _]}, socket) do
    # ignore change events from the editor field
    {:noreply, socket}
  end

  def parse_query(query_string, endpoints, alerts)
  def parse_query("", _endpoints, _alerts), do: :ok

  def parse_query(query_string, endpoints, alerts) do
    case Logflare.Endpoints.parse_query_string(
           :bq_sql,
           query_string,
           endpoints,
           alerts
         ) do
      {:ok, _} ->
        :ok

      {:error, "sql parser error: " <> message} ->
        {:error, message}

      {:error, err} ->
        message = if(is_binary(err), do: err, else: inspect(err))
        {:error, message}
    end
  end

  defp editor_opts do
    Map.merge(
      LiveMonacoEditor.default_opts(),
      %{
        "wordWrap" => "on",
        "language" => "sql",
        "fontSize" => 12,
        "padding" => %{
          "top" => 14,
          "bottom" => 14
        },
        "contextmenu" => false,
        "hideCursorInOverviewRuler" => true,
        "smoothScrolling" => true,
        "scrollbar" => %{
          "vertical" => "auto",
          "horizontal" => "hidden",
          "verticalScrollbarSize" => 6
        },
        "lineNumbers" => "off",
        "glyphMargin" => false,
        "lineNumbersMinChars" => 0,
        "folding" => false,
        "roundedSelection" => true,
        "minimap" => %{
          "enabled" => false
        }
      }
    )
  end
end
