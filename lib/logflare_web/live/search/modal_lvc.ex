defmodule LogflareWeb.Source.SearchLV.ModalLVC do
  @moduledoc """
  LiveView Component to render components
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.Helpers.BqSchema
  alias LogflareWeb.SharedView
  alias LogflareWeb.Source.SearchLV
  alias Logflare.Sources
  alias Logflare.Logs.LogEvents
  require Logger

  @query_debug_modals ~w(queryDebugEventsModal queryDebugErrorModal queryDebugAggregatesModal)

  def render(%{active_modal: "searchHelpModal"} = _assigns) do
    SharedView.render("modal.html",
      title: "Logflare Query Language",
      body: SharedView.render("lql_help.html"),
      type: "lql-help-modal"
    )
  end

  def render(%{active_modal: "sourceSchemaModal"} = assigns) do
    bq_schema = Sources.Cache.get_bq_schema(assigns.source)

    SharedView.render("modal.html",
      title: "Source Schema",
      body: BqSchema.format_bq_schema(bq_schema),
      type: "source-schema-modal"
    )
  end

  def render(%{active_modal: "metadataModal:" <> _, metadata_modal_log_event: le}) do
    fmt_metadata = BqSchema.encode_metadata(le.body.metadata)

    body =
      SharedView.render("metadata_modal_body.html",
        log_event: le,
        fmt_metadata: fmt_metadata
      )

    SharedView.render("modal.html",
      title: "Metadata",
      body: body,
      type: "metadata-modal"
    )
  end

  def render(%{active_modal: "metadataModal:" <> id_and_timestamp} = assigns) do
    [id, timestamp] = String.split(id_and_timestamp, "|")

    timestamp = timestamp |> String.to_integer() |> DateTime.from_unix!(:microsecond)

    pid = self()

    Task.start(fn ->
      with {:ok, log_event} <-
             LogEvents.Cache.fetch_event_by_id_and_timestamp(assigns.source.token,
               id: id,
               timestamp: timestamp
             ) do
        send(
          pid,
          {:phoenix, :send_update,
           {__MODULE__, :active_modal, %{metadata_modal_log_event: log_event}}}
        )
      else
        {:error, error} ->
          case error do
            :not_found ->
              Logger.error("Log event not found for id #{id} and timestamp #{timestamp}")

            e ->
              Logger.error("Error: #{inspect(e)}")
          end
      end
    end)

    SharedView.render("modal.html",
      title: "Metadata",
      body: SharedView.render("loader.html"),
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

    SharedView.render(
      "modal.html",
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
