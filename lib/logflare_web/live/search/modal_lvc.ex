defmodule LogflareWeb.Source.SearchLV.ModalLVC do
  @moduledoc """
  LiveView Component to render components
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView
  alias LogflareWeb.Source

  def render(assigns) do
    log_events = assigns[:log_events]
    source = assigns[:source]
    active_modal = assigns[:active_modal]

    case active_modal do
      "searchHelpModal" ->
        SearchView.render("modal.html",
          id: "searchHelpModal",
          title: "Logflare Query Language",
          body: SearchView.render("lql_help.html")
        )

      "sourceSchemaModal" ->
        SearchView.render("modal.html",
          id: "sourceSchemaModal",
          title: "Source Schema",
          body: SearchView.format_bq_schema(source)
        )

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

        SearchView.render("modal.html",
          id: "metadataModal",
          title: "Metadata",
          body: body
        )

      modal
      when modal in ~w(queryDebugEventsModal queryDebugErrorModal queryDebugAggregatesModal) ->
        search_op =
          case modal do
            "queryDebugEventsModal" -> assigns.search_op_log_events
            "queryDebugAggregatesModal" -> assigns.search_op_log_aggregates
            "queryDebugErrorModal" -> assigns.search_op_error
          end

        SearchView.render("modal.html",
          id: modal,
          title: "Query Debugging",
          body: ~L"<%= live_component(@socket, Source.SearchLV.DebugLVC, search_op: search_op) %>"
        )

      _ ->
        ~L"""
        <div class="source-logs-search-modals"> </div>
        """
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
