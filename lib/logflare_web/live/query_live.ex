defmodule LogflareWeb.QueryLive do
  @moduledoc false
  use LogflareWeb, :live_view
  use Phoenix.Component

  require Logger

  alias Logflare.Endpoints
  alias Logflare.Alerting
  alias Logflare.Users
  alias Logflare.Backends
  alias LogflareWeb.QueryComponents

  def render(assigns) do
    ~H"""
    <.subheader>
      <:path>
        ~/<.subheader_path_link live_patch to={~p"/query"}>query</.subheader_path_link>
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
      <.form for={%{}} phx-submit="run-query" class="tw-min-h-[80px] tw-flex tw-flex-col tw-gap-4">
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
          <QueryComponents.query_cost bytes={@total_bytes_processed} />
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

  def mount(%{}, %{"user_id" => user_id}, socket) do
    user = Users.get(user_id)

    endpoints = Endpoints.list_endpoints_by(user_id: user.id)
    alerts = Alerting.list_alert_queries_by_user_id(user.id)

    socket =
      socket
      |> assign(:user_id, user_id)
      |> assign(:user, user)
      |> assign(:query_result_rows, nil)
      |> assign(:total_bytes_processed, nil)
      |> assign(:parse_error_message, nil)
      |> assign(:query_string, nil)
      |> assign(:endpoints, endpoints)
      |> assign(:alerts, alerts)

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

    query_string =
      if q != nil and socket.assigns.query_string == nil do
        q
      else
        "SELECT id, timestamp, metadata, event_message \nFROM `YourSource` \nWHERE timestamp > '#{DateTime.utc_now() |> DateTime.to_iso8601()}'"
      end

    if query_string != nil do
      send(self(), :parse_query)
    end

    {:noreply, assign(socket, :query_string, query_string)}
  end

  def handle_info(:parse_query, socket) do
    query_string = socket.assigns.query_string

    socket =
      case Endpoints.parse_query_string(
             :bq_sql,
             query_string,
             socket.assigns.endpoints,
             socket.assigns.alerts
           ) do
        {:ok, _} ->
          socket
          |> assign(:parse_error_message, nil)

        {:error, err} ->
          error = if(is_binary(err), do: err, else: inspect(err))

          socket
          |> assign(:parse_error_message, error)
      end

    {:noreply, socket}
  end

  def handle_event(
        "run-query",
        _params,
        %{assigns: %{user: user, query_string: query_string}} = socket
      ) do
    socket =
      run_query(socket, user, query_string)
      |> push_patch(to: ~p"/query?#{%{q: query_string}}")

    {:noreply, socket}
  end

  def handle_event(
        "parse-query",
        %{"value" => query_string},
        socket
      ) do
    send(self(), :parse_query)

    socket =
      socket
      |> assign(:query_string, query_string)

    handle_info(:parse_query, socket)
  end

  def handle_event("parse-query", %{"_target" => ["live_monaco_editor", _]}, socket) do
    # ignore change events from the editor field
    {:noreply, socket}
  end

  def handle_event("format-query", _params, socket) do
    {:ok, formatted} = SqlFmt.format_query(socket.assigns.query_string)
    {:noreply, LiveMonacoEditor.set_value(socket, formatted, to: "query")}
  end

  defp run_query(socket, user, query_string) do
    type =
      case Backends.get_default_backend(user) do
        %_{type: :bigquery} -> :bq_sql
        %_{type: :postgres} -> :pg_sql
      end

    case Endpoints.run_query_string(user, {type, query_string},
           params: %{},
           use_query_cache: false
         ) do
      {:ok, %{rows: rows, total_bytes_processed: total_bytes_processed}} ->
        socket
        |> put_flash(:info, "Ran query successfully")
        |> assign(:query_result_rows, rows)
        |> assign(:total_bytes_processed, total_bytes_processed)

      {:error, err} ->
        socket
        |> put_flash(:error, "Error occurred when running query: #{inspect(err)}")
    end
  end
end
