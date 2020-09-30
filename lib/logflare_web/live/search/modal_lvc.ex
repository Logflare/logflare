defmodule LogflareWeb.Source.SearchLV.ModalLVC do
  @moduledoc """
  LiveView Component to render components
  """
  use Phoenix.LiveComponent
  alias LogflareWeb.Helpers.BqSchema
  alias LogflareWeb.{SharedView, LogView}
  alias LogflareWeb.Source.SearchLV
  alias Logflare.Sources
  alias Logflare.Logs.LogEvents
  alias LogflareWeb.LogView
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

  def render(
        %{active_modal: "metadataModal:" <> id_and_timestamp, metadata_modal_log_event: le} =
          assigns
      ) do
    [id, _timestamp] = String.split(id_and_timestamp, "|")

    if is_nil(le) or le.id != id do
      render(Map.delete(assigns, :metadata_modal_log_event))
    else
      m = le.body.metadata

      body =
        LogView.render("log_event_body.html",
          source: assigns.source,
          metadata: m,
          fmt_metadata: BqSchema.encode_metadata(m),
          message: le.body.message,
          id: le.id,
          timestamp: Timex.from_unix(le.body.timestamp, :microsecond),
          user_local_timezone: nil
        )

      SharedView.render("modal.html",
        title: "Metadata",
        body: body,
        type: "metadata-modal"
      )
    end
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
    ~L"<div></div>"
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
