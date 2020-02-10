defmodule LogflareWeb.Source.SearchLV.ModalLVC do
  @moduledoc """
  LiveView Component to render components
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView
  alias LogflareWeb.Source.SearchLV
  @query_debug_modals ~w(queryDebugEventsModal queryDebugErrorModal queryDebugAggregatesModal)

  def render(assigns) do
    log_events = assigns[:log_events]
    source = assigns[:source]
    active_modal = assigns[:active_modal]

    assigns =
      case active_modal do
        "searchHelpModal" ->
          [
            title: "Logflare Query Language",
            body: SearchView.render("lql_help.html"),
            type: "lql-help-modal"
          ]

        "sourceSchemaModal" ->
          [
            title: "Source Schema",
            body: SearchView.format_bq_schema(source),
            type: "source-schema-modal"
          ]

        "metadataModal:" <> id ->
          log_event =
            Enum.find(log_events, &(&1.id === id)) ||
              Enum.find(log_events, &("#{&1.body.timestamp}" === id))

          fmt_metadata =
            log_event
            |> Map.get(:body)
            |> Map.get(:metadata)
            |> SearchView.encode_metadata()

          body =
            SearchView.render("metadata_modal_body.html",
              log_event: log_event,
              fmt_metadata: fmt_metadata
            )

          [
            title: "Metadata",
            body: body,
            type: "metadata-modal"
          ]

        modal when modal in @query_debug_modals ->
          search_op =
            case modal do
              "queryDebugEventsModal" -> assigns.search_op_log_events
              "queryDebugAggregatesModal" -> assigns.search_op_log_aggregates
              "queryDebugErrorModal" -> assigns.search_op_error
            end

          [
            title: "Query Debugging",
            body: ~L"<%= live_component(@socket, SearchLV.DebugLVC, search_op: search_op) %>",
            type: "search-op-debug-modal"
          ]

        _ ->
          []
      end

    if not Enum.empty?(assigns) do
      SearchView.render("modal.html", assigns)
    else
      ~L""
    end
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
