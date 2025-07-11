defmodule LogflareWeb.MonacoEditorComponent do
  use LogflareWeb, :live_component

  def mount(socket) do
    {:ok, assign(socket, parse_error_message: nil)}
  end

  def render(assigns) do
    assigns = assigns |> assign(editor_opts: editor_opts())

    ~H"""
    <div>
      <LiveMonacoEditor.code_editor value={@field.value} target={@myself} change="parse-query" path="query_string" id="query" data-completions-name={[@field.name, "_completions"]} opts={@editor_opts} />
      <input type="hidden" name={[@field.name, "_completions"]} value={Jason.encode!(@completions)} />
      <%= hidden_input(@field.form, :query, value: @field.value) %>
      <%= error_tag(@field.form, :query) %>
      <.alert :if={@parse_error_message} variant="warning">
        <strong>SQL Parse error!</strong>
        <br />
        <span><%= @parse_error_message %></span>
      </.alert>
    </div>
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
