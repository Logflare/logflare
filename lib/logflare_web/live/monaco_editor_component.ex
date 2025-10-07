defmodule LogflareWeb.MonacoEditorComponent do
  use LogflareWeb, :live_component

  @moduledoc """
  Provides a Monaco Editor component for creating LQL queries.

  Expects following attributes:
    - `field`: a `Phoenix.HTML.FormField` for the query.
    - `endpoints`: A list of endpoints, used for validating LQL queries and completions.
    - `alerts`: A list of alerts, used for validating LQL queries and completions.
    - `sources`: A list of sources, used for completions.
    - `id`: A unique identifier for the editor.
    - `on_query_change`: Optional function to be called when the query changes.

  ## Example

        <.live_component module={LogflareWeb.MonacoEditorComponent}
          id="endpoint_query"
          field={f[:query]}
          endpoints={@endpoints}
          alerts={@alerts}
          sources={@sources}
          on_query_change={fn query_string -> send(self(), {:query_string_updated, query_string}) end}
          />
  """

  def mount(socket) do
    {:ok, assign(socket, parse_error_message: nil, on_query_change: nil, query: nil)}
  end

  def update(assigns, socket) do
    # only use field if not first mount
    assigns_query = socket.assigns.query

    query =
      case assigns do
        %{field: %{value: query}} when assigns_query == nil -> query
        _ -> assigns_query || ""
      end

    {:ok,
     socket
     |> assign(assigns)
     |> assign(:query, query)
     |> assign_completions()}
  end

  def render(assigns) do
    assigns = assigns |> assign(editor_opts: editor_opts())

    ~H"""
    <div id={@id}>
      <.support_scripts completions={@completions} id={@field.id <> "_completions"} />
      <LiveMonacoEditor.code_editor value={@field.value} target={@myself} change="parse-query" path="query_string" phx-format={JS.hide()} id={"#{@id}_lme"} opts={@editor_opts} />
      <div class="tw-flex tw-justify-end">
        <button type="button" class="btn btn-secondary tw-mr-0 tw-mt-2" phx-click={JS.dispatch("editor:format", to: "#" <> @id)}>
          Format
        </button>
      </div>

      <%= hidden_input(@field.form, :query, value: @query) %>
      <%= error_tag(@field.form, :query) %>
      <.alert :if={@parse_error_message} variant="warning">
        <strong>SQL Parse error!</strong>
        <br />
        <span><%= @parse_error_message %></span>
      </.alert>
    </div>
    """
  end

  attr :completions, :list, required: true, examples: [["source_name", "logs"]]
  attr :id, :string, required: true

  def support_scripts(assigns) do
    ~H"""
    <script phx-update="ignore" id={@id}>
      window.addEventListener("lme:editor_mounted", (event) => {
        const hook = event.detail.hook;
        const editor = event.detail.editor.standalone_code_editor;

        window.addEventListener("editor:format", ev => {
          const value = editor.getValue();
          hook.pushEventTo(ev.target, "format-query", { value: value });
        })

        const completions = <%= Jason.encode!(@completions) |> raw() %>

        if (completions.length == 0) { return; }

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

  def handle_event("format-query", %{"value" => value}, socket) do
    case SqlFmt.format_query(value) do
      {:ok, formatted} ->
        {:noreply, socket |> LiveMonacoEditor.set_value(formatted, to: "query_string")}

      _ ->
        {:noreply, socket}
    end
  end

  def handle_event("parse-query", %{"value" => query}, socket) do
    %{endpoints: endpoints, alerts: alerts} = socket.assigns

    if is_function(socket.assigns.on_query_change) do
      socket.assigns.on_query_change.(query)
    end

    field = socket.assigns.field |> Map.put(:value, query)

    socket =
      case parse_query(query, endpoints, alerts) do
        :ok ->
          assign(socket, parse_error_message: nil)

        {:error, message} ->
          assign(socket, parse_error_message: message)
      end
      |> assign(:query, query)
      |> assign(:field, field)

    {:noreply, socket}
  end

  def handle_event("parse-query", %{"_target" => ["live_monaco_editor", _]}, socket) do
    # ignore change events from the editor field
    {:noreply, socket}
  end

  @spec parse_query(String.t(), [%Logflare.Endpoints.Query{}], [%Logflare.Alerting.AlertQuery{}]) ::
          :ok | {:error, String.t()}
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

  defp assign_completions(socket) do
    %{endpoints: endpoints, alerts: alerts, sources: sources} = socket.assigns

    completions =
      [sources, endpoints, alerts] |> List.flatten() |> Enum.map(fn item -> item.name end)

    assign(socket, completions: completions)
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
