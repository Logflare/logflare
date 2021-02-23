defmodule LogflareWeb.Search.LogEventViewerComponent do
  use LogflareWeb, :live_component
  alias LogflareWeb.LogView
  require Logger
  alias LogflareWeb.Helpers.BqSchema
  use Logflare.Commons
  alias LogflareWeb.SharedView

  @impl true
  def render(%{log_event: %LE{body: %{metadata: metadata}, source: %Source{}} = le} = assigns) do
    LogView.render("log_event_body.html",
      source: le.source,
      metadata: metadata,
      fmt_metadata: BqSchema.encode_metadata(metadata),
      message: le.body.message,
      id: le.id,
      timestamp: Timex.from_unix(le.body.timestamp, :microsecond),
      user_local_timezone: nil
    )
  end

  @impl true
  def render(_assigns) do
    SharedView.render("loader.html")
  end

  @impl true
  def update(%{log_event: {:error, _}}, socket) do
    {:ok, assign(socket, :error, "Error!")}
  end

  @impl true
  def update(%{log_event: log_event_result}, socket) do
    {:ok, assign(socket, :log_event, log_event_result)}
  end

  @impl true
  def update(%{origin: origin, id_param: log_id, source: source, path: path} = assigns, socket) do
    d = Date.utc_today()
    dminus3 = Timex.shift(d, days: -3)
    dplus1 = Timex.shift(d, days: 1)

    le =
      case origin do
        "vercel" ->
          LogEvents.get_log_event_by_metadata_for_source(%{"id" => log_id}, source.token)
      end

    socket =
      if le do
        socket
        |> assign(Map.delete(assigns, :flash))
        |> assign(:log_event, le)
      else
        start_task(path: path, value: log_id, partitions_range: [dminus3, dplus1], source: source)
        assign(socket, Map.delete(assigns, :flash))
      end

    socket = assign(socket, source: source)
    {:ok, socket}
  end

  @impl true
  def update(assigns, socket) do
    %{"log-event-id" => id, "log-event-timestamp" => timestamp} = assigns.params

    d = String.to_integer(timestamp) |> Timex.from_unix(:microsecond) |> Timex.to_date()
    dminus1 = Timex.shift(d, days: -1)
    dplus1 = Timex.shift(d, days: +1)

    le = LogEvents.get_log_event(id)

    socket =
      if le do
        socket
        |> assign(Map.delete(assigns, :flash))
        |> assign(:log_event, le)
      else
        start_task(uuid: id, partitions_range: [dminus1, dplus1], source: assigns.source)
        assign(socket, Map.delete(assigns, :flash))
      end

    {:ok, socket}
  end

  @spec params_to_cache_key(map()) :: {String.t(), String.t()}
  defp params_to_cache_key(%{uuid: id}) do
    {"uuid", id}
  end

  defp params_to_cache_key(%{path: path, value: value}) do
    {path, value}
  end

  defp start_task(params) do
    params = Enum.into(params, %{})
    source = params.source
    pid = self()

    Task.start(fn ->
      case params do
        %{uuid: id} ->
          LogEvents.fetch_event_by_id(source.token, id, partitions_range: params.partitions_range)

        %{id: id} ->
          LogEvents.fetch_event_by_id(source.token, id, partitions_range: params.partitions_range)

        %{path: "uuid", value: id} ->
          LogEvents.fetch_event_by_id(source.token, id, partitions_range: params.partitions_range)

        %{path: path, value: value} ->
          LogEvents.fetch_event_by_path(source.token, path, value)
      end
      |> case do
        %{} = bq_row ->
          le = LE.make_from_db(bq_row, %{source: source})

          LogEvents.Cache.put(
            source.token,
            params_to_cache_key(params),
            le
          )

          send_update(pid, __MODULE__, log_event: le, id: :log_event_viewer)

        {:error, error} ->
          error =
            case error do
              :not_found ->
                [from, to] = params.partitions_range

                err = "Log event with id #{params[:id]} between #{from} and #{to} was not found"
                Logger.warn(err)
                err

              e ->
                Logger.error("Error: #{inspect(e)}")

                "Oops, something went wrong!"
            end

          send_update(pid, __MODULE__, error: error, id: :log_event_viewer)
      end
    end)
  end
end
