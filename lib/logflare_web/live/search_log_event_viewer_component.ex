defmodule LogflareWeb.Search.LogEventViewerComponent do
  use LogflareWeb, :live_component
  alias LogflareWeb.LogView
  require Logger
  alias LogflareWeb.Helpers.BqSchema

  @impl true
  def render(%{log_event: le = %{body: %{metadata: md}}} = assigns) do
    LogView.render("log_event_body.html",
      source: assigns.source,
      metadata: md,
      fmt_metadata: BqSchema.encode_metadata(md),
      message: le.body.message,
      id: le.id,
      timestamp: Timex.from_unix(le.body.timestamp, :microsecond),
      user_local_timezone: nil
    )
  end

  def update(%{"log_event_id" => id, "log_event_timestamp" => timestamp} = assigns, socket) do
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
end
