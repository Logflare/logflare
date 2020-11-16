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
  alias Logflare.LogEvent, as: LE
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

  def render(%{active_modal: "metadataModal:" <> _, metadata_modal_log_event: nil}) do
    SharedView.render("modal.html",
      title: "Metadata",
      body: SharedView.render("loader.html"),
      type: "metadata-modal"
    )
  end

  def render(%{active_modal: "metadataModal:" <> _, metadata_modal_log_event: le} = assigns)
      when not is_nil(le) do
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

  def update(%{active_modal: "metadataModal:" <> id_and_timestamp} = assigns, socket) do
    [id, timestamp] = String.split(id_and_timestamp, "|")
    d = String.to_integer(timestamp) |> Timex.from_unix(:microsecond) |> Timex.to_date()
    dminus1 = Timex.shift(d, days: -1)
    dplus1 = Timex.shift(d, days: +1)

    token = assigns.source.token
    le = LogEvents.Cache.get!(token, {"uuid", id})

    pid = self()

    if is_nil(le) do
      Task.start(fn ->
        token
        |> LogEvents.fetch_event_by_id(id, partitions_range: [dminus1, dplus1])
        |> case do
          %{} = bq_row ->
            le = LE.make_from_db(bq_row, %{source: assigns.source})

            LogEvents.Cache.put(
              token,
              {"uuid", id},
              le
            )

            send(
              pid,
              {:phoenix, :send_update,
               {__MODULE__, :active_modal, %{metadata_modal_log_event: le}}}
            )

          {:error, error} ->
            case error do
              :not_found ->
                Logger.error("Log event with id #{id} not found")

              e ->
                Logger.error("Error: #{inspect(e)}")
            end
        end
      end)
    end

    socket =
      socket
      |> assign(assigns)
      |> assign(:metadata_modal_log_event, le)

    {:ok, socket}
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
