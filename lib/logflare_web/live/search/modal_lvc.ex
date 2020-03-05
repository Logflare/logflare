defmodule LogflareWeb.Source.SearchLV.ModalLVC do
  @moduledoc """
  LiveView Component to render components
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView
  alias LogflareWeb.Source.SearchLV
  alias Logflare.Logs

  @query_debug_modals ~w(queryDebugEventsModal queryDebugErrorModal queryDebugAggregatesModal)

  def render(%{active_modal: "searchHelpModal"} = _assigns) do
    SearchView.render("modal.html",
      title: "Logflare Query Language",
      body: SearchView.render("lql_help.html"),
      type: "lql-help-modal"
    )
  end

  def render(%{active_modal: "sourceSchemaModal"} = assigns) do
    SearchView.render("modal.html",
      title: "Source Schema",
      body: SearchView.format_bq_schema(assigns.source),
      type: "source-schema-modal"
    )
  end

  def render(%{active_modal: "metadataModal:" <> id_and_timestamp} = assigns) do
    [id, timestamp] = String.split(id_and_timestamp, "|")

    log_events = assigns.log_events
    timestamp = String.to_integer(timestamp) |> DateTime.from_unix!(:microsecond)

    log_event =
      Logs.Search.get_event_by_id_and_timestamp( assigns.source, id: id, timestamp: timestamp)

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
      title: "Metadata",
      body: body,
      type: "metadata-modal"
    )
  end

  def render(%{active_modal: modal} = assigns) when modal in @query_debug_modals do
    search_op =
      case modal do
        "queryDebugEventsModal" -> assigns.search_op_log_events
        "queryDebugAggregatesModal" -> assigns.search_op_log_aggregates
        "queryDebugErrorModal" -> assigns.search_op_error
      end

    SearchView.render("modal.html",
      title: "Query Debugging",
      body:
        ~L"<%= live_component(@socket, SearchLV.DebugLVC, search_op: search_op, user: @user) %>",
      type: "search-op-debug-modal"
    )
  end

  def render(assigns) do
    ~L""
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
