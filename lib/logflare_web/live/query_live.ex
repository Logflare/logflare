defmodule LogflareWeb.QueryLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  require Logger

  alias Logflare.Alerting
  alias Logflare.Backends
  alias Logflare.Endpoints
  alias Logflare.Sources
  alias Logflare.Sql
  alias LogflareWeb.AuthLive
  alias LogflareWeb.QueryComponents

  def render(assigns) do
    ~H"""
    <.subheader>
      <:path>
        ~/<.subheader_path_link live_patch to={~p"/query"} team={@team}>query</.subheader_path_link>
      </:path>
    </.subheader>

    <section class="pt-2 mx-auto container">
      <p>
        Query your data with BigQuery SQL directly. You can refer to source names directly in your SELECT queries, for example <br />
        <code>SELECT datetime(timestamp) as timestamp, event_message, metadata from `MyApp.Logs`</code>
        Some pointers:
        <ul>
          <li>Always have a filter over the <code>timestamp</code> column in your <code>WHERE</code> clause</li>
          <li>Use <code>CROSS JOIN UNNEST(my_table.my_column) as col</code> to use nested fields in your query</li>
          <li>Smaller time ranges load faster</li>
          <li>Endpoint and alert queries can be referenced using <code>`MyEndpointName`</code> for query composition</li>
        </ul>
        <a href="https://docs.logflare.app/backends/bigquery#querying-in-bigquery">Read the docs</a>
        to find out more about querying Logflare with BigQuery SQL
      </p>
    </section>
    <section class="mx-auto container pt-3 tw-flex tw-flex-col tw-gap-4">
      <.form for={@form} id="query-form" phx-submit="run-query" phx-change="set_backend" class="tw-min-h-[80px] tw-flex tw-flex-col tw-gap-4">
        <QueryComponents.backend_select :if={Enum.any?(@backends)} backends={@backends} form={@form}>
          <:help>Choose which backend to execute this query against.</:help>
        </QueryComponents.backend_select>
        <LiveMonacoEditor.code_editor
          value={@query_string}
          change="parse-query"
          path="query"
          id="query"
          opts={
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
                  "verticalScrollbarSize" => 6,
                  "alwaysConsumeMouseWheel" => false
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
          }
        />
        <div class="tw-ml-auto">
          <button type="button" class="btn btn-secondary" phx-click="format-query">
            Format
          </button>
          {submit("Run query", class: "btn btn-secondary")}
        </div>
      </.form>

      <div :if={@parse_error_message}>
        <.alert variant="warning">
          <strong>SQL Parse error!</strong>
          <br />
          <span>{@parse_error_message}</span>
        </.alert>
      </div>
    </section>

    <section :if={@query_result_rows} class="container mx-auto">
      <div class="tw-flex tw-justify-between tw-items-end">
        <h3>Query result</h3>
        <div class="tw-mb-1">
          <QueryComponents.query_cost :if={is_number(@total_bytes_processed)} bytes={@total_bytes_processed} />
        </div>
      </div>
      <p :if={@query_result_rows == []}>
        No rows returned from query. Try adjusting your query and try again!
      </p>
      <div :if={@query_result_rows != []}>
        <% keys = Map.keys(hd(@query_result_rows)) |> Enum.sort() %>
        <table class="table table-bordered table-dark table-sm table-hover table-responsive tw-overflow-x-auto  tw-w-[95vw] tw-font-mono tw-text-[0.7rem]">
          <thead>
            <tr>
              <th :for={k <- keys} scope="col" class="tw-w-max-[50vw]">{k}</th>
            </tr>
          </thead>
          <tbody>
            <tr :for={{row, row_idx} <- Enum.with_index(@query_result_rows)}>
              <td :for={{k, col_idx} <- Enum.with_index(keys)}>
                <%= case value = Map.get(row, k) do %>
                  <% value when is_map(value) or is_list(value) -> %>
                    <button type="button" class="btn btn-link tw-truncate tw-text-ellipsis tw-w-32 tw-text-[0.7rem]" data-toggle="modal" data-target={"#modal-#{row_idx}-#{col_idx}"}>
                      {Jason.encode!(value) |> String.slice(0..150)}
                    </button>
                  <% value -> %>
                    <span class="tw-max-w-[50vw]  tw-block tw-text-wrap">
                      {value}
                    </span>
                <% end %>

                <div class="modal fade" id={"modal-#{row_idx}-#{col_idx}"} data-backdrop="static" data-keyboard="false" tabindex="-1" aria-labelledby={"staticBackdropLabel-#{k}"} aria-hidden="true">
                  <div class="modal-dialog modal-xl">
                    <div class="modal-content">
                      <div class="modal-header">
                        <h5 class="modal-title" id={"staticBackdropLabel-#{k}"}>{k}</h5>
                        <button type="button" class="close" data-dismiss="modal" aria-label="Close">
                          <span aria-hidden="true">&times;</span>
                        </button>
                      </div>
                      <div class="modal-body">
                        <pre class="tw-text-[0.7rem] tw-p-2"><%= Jason.encode!(value, pretty: true) %></pre>
                      </div>
                      <div class="modal-footer">
                        <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
                        <button type="button" class="btn btn-primary" phx-click={JS.dispatch("logflare:copy-to-clipboard", detail: %{text: Jason.encode!(value)})} data-toggle="tooltip" data-placement="top" title="Copy to clipboard"><i class="fa fa-clone" aria-hidden="true"></i> Copy</button>
                      </div>
                    </div>
                  </div>
                </div>
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </section>
    """
  end

  def mount(%{}, _session, socket) do
    %{assigns: %{user: user}} = socket

    endpoints = Endpoints.list_endpoints_by(user_id: user.id)
    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    socket =
      socket
      |> assign(:user_id, user.id)
      |> assign(:query_result_rows, nil)
      |> assign(:total_bytes_processed, nil)
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:endpoints, endpoints)
      |> assign(:alerts, alerts)
      |> assign_backends()
      |> assign_form(%{})

    {:ok, socket}
  end

  def handle_params(params, _uri, socket) do
    q =
      case params["q"] do
        v when v in ["", nil] ->
          nil

        v ->
          {:ok, formatted} = SqlFmt.format_query(v)
          formatted
      end

    socket = maybe_assign_team_context(socket, params, q)

    %{assigns: %{user: user}} = socket
    endpoints = Endpoints.list_endpoints_by(user_id: user.id)
    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    socket =
      socket
      |> assign(:user_id, user.id)
      |> assign(:endpoints, endpoints)
      |> assign(:alerts, alerts)
      |> assign_form(params)

    query_string =
      q ||
        socket.assigns.query_string ||
        "SELECT id, timestamp, metadata, event_message \nFROM YourSource \nWHERE timestamp > '#{DateTime.utc_now() |> DateTime.to_iso8601()}'"

    if query_string != nil do
      send(self(), :parse_query)
    end

    {:noreply, assign(socket, :query_string, query_string)}
  end

  @spec assign_backends(Phoenix.LiveView.Socket.t()) :: Phoenix.LiveView.Socket.t()
  defp assign_backends(socket) do
    %{user: user} = socket.assigns

    backends =
      Backends.list_backends_by_user_id(user.id)
      |> Enum.filter(&Backends.Adaptor.can_query?/1)

    socket
    |> assign(:backends, backends)
  end

  defp assign_form(socket, params) do
    form =
      params
      |> Map.take(["backend_id"])
      |> to_form(as: :backend)

    assign(socket, :form, form)
  end

  @spec get_selected_backend(Phoenix.LiveView.Socket.t()) :: Backends.Backend.t() | nil
  defp get_selected_backend(socket) do
    backend_id = Phoenix.HTML.Form.input_value(socket.assigns.form, :backend_id)

    case parse_backend_id(backend_id) do
      id when is_integer(id) ->
        Enum.find(socket.assigns.backends, &(&1.id == id))

      _ ->
        nil
    end
  end

  defp maybe_assign_team_context(socket, %{"t" => _team_id}, _query), do: socket

  defp maybe_assign_team_context(socket, _params, nil), do: socket

  defp maybe_assign_team_context(socket, _params, query_string) do
    effective_user = socket.assigns[:team_user] || socket.assigns.user

    with {:ok, [source_name | _]} <- Sql.extract_table_names(query_string),
         %Sources.Source{} = source <-
           Sources.get_by_name_and_user_access(effective_user, source_name) do
      AuthLive.assign_context_by_resource(socket, source, socket.assigns.user.email)
    else
      _ -> socket
    end
  end

  def handle_info(:parse_query, socket) do
    {:noreply, parse_query(socket)}
  end

  def handle_event(
        "run-query",
        params,
        %{assigns: %{query_string: query_string}} = socket
      ) do
    socket =
      socket
      |> maybe_assign_team_context(%{}, query_string)

    %{assigns: %{user: user}} = socket

    patch_params =
      socket
      |> build_params(params["backend"])

    socket =
      socket
      |> assign(:user_id, user.id)
      |> run_query(user, query_string)
      |> push_patch(to: ~p"/query?#{patch_params}")

    {:noreply, socket}
  end

  def handle_event(
        "parse-query",
        %{"value" => query_string},
        socket
      ) do
    socket =
      socket
      |> assign(:query_string, query_string)

    {:noreply, parse_query(socket)}
  end

  def handle_event("parse-query", %{"_target" => ["live_monaco_editor", _]}, socket) do
    # ignore change events from the editor field
    {:noreply, socket}
  end

  def handle_event("format-query", _params, socket) do
    {:ok, formatted} = SqlFmt.format_query(socket.assigns.query_string)
    {:noreply, LiveMonacoEditor.set_value(socket, formatted, to: "query")}
  end

  def handle_event("set_backend", %{"backend" => params}, socket) do
    {:noreply, assign_form(socket, params)}
  end

  defp run_query(socket, user, query_string) do
    backend = get_selected_backend(socket)
    language = Logflare.Endpoints.Query.map_backend_to_language(backend, false)

    case Endpoints.run_query_string(user, {language, query_string},
           params: %{},
           use_query_cache: false,
           backend_id: backend && backend.id
         ) do
      {:ok, %{rows: rows, total_bytes_processed: total_bytes_processed}} ->
        socket
        |> put_flash(:info, "Ran query successfully")
        |> assign(:query_result_rows, rows)
        |> assign(:total_bytes_processed, total_bytes_processed)

      {:ok, %{rows: rows}} ->
        socket
        |> put_flash(:info, "Ran query successfully")
        |> assign(:query_result_rows, rows)
        |> assign(:total_bytes_processed, nil)

      {:error, err} ->
        socket
        |> put_flash(:error, "Error occurred when running query: #{inspect(err)}")
    end
  end

  defp build_params(%{assigns: assigns} = _socket, params) do
    %{
      "q" => assigns.query_string,
      "t" => assigns.team.id,
      "backend_id" => params["backend_id"]
    }
    |> Map.reject(fn {_key, value} -> value in [nil, ""] end)
  end

  @spec parse_backend_id(String.t() | nil) :: integer() | nil
  defp parse_backend_id(nil), do: nil

  defp parse_backend_id(id) do
    case Integer.parse(id) do
      {id, _} when is_number(id) -> id
      _ -> nil
    end
  end

  @spec parse_query(Phoenix.LiveView.Socket.t()) :: Phoenix.LiveView.Socket.t()
  defp parse_query(%{assigns: %{query_string: query_string}} = socket)
       when not is_binary(query_string) do
    socket
  end

  defp parse_query(socket) do
    backend = get_selected_backend(socket)
    language = Logflare.Endpoints.Query.map_backend_to_language(backend, false)

    case Endpoints.parse_query_string(
           language,
           socket.assigns.query_string,
           socket.assigns.endpoints,
           socket.assigns.alerts
         ) do
      {:ok, _} ->
        assign(socket, :parse_error_message, nil)

      {:error, err} ->
        assign(socket, :parse_error_message, err)
    end
  end
end
