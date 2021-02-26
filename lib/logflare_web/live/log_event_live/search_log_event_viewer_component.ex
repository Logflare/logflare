defmodule LogflareWeb.Search.LogEventViewerComponent do
  use LogflareWeb, :live_component
  alias LogflareWeb.LogView
  require Logger
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.Logs.LogEvents
  alias Logflare.LogEvent, as: LE
  alias LogflareWeb.SharedView

  @impl true
  def update(%{log_event: {:error, _}} = assigns, socket) do
    socket =
      assign(socket, :error, "Error!")
      |> assign_defaults(assigns)

    {:ok, socket}
  end

  @impl true
  def update(%{log_event: log_event_result} = assigns, socket) do
    socket =
      assign(socket, :log_event, log_event_result)
      |> assign_defaults(assigns)

    {:ok, socket}
  end

  @impl true
  def update(%{origin: _origin, id_param: log_id, source: source, path: path} = assigns, socket) do
    d = Date.utc_today()
    dminus3 = Timex.shift(d, days: -3)
    dplus1 = Timex.shift(d, days: 1)

    le = LogEvents.Cache.get!(source.token, params_to_cache_key(%{path: path, value: log_id}))

    socket =
      if le do
        socket
        |> assign(Map.delete(assigns, :flash))
        |> assign(:log_event, le)
      else
        start_task(path: path, value: log_id, partitions_range: [dminus3, dplus1], source: source)
        assign(socket, Map.delete(assigns, :flash))
      end

    socket = assign_defaults(socket, assigns)

    {:ok, socket}
  end

  @impl true
  def update(assigns, socket) do
    %{"log-event-id" => id, "log-event-timestamp" => timestamp} = assigns.params

    d = String.to_integer(timestamp) |> Timex.from_unix(:microsecond) |> Timex.to_date()
    dminus1 = Timex.shift(d, days: -1)
    dplus1 = Timex.shift(d, days: +1)

    token = assigns.source.token
    le = LogEvents.Cache.get!(token, params_to_cache_key(%{uuid: id}))

    socket =
      if le do
        socket
        |> assign(Map.delete(assigns, :flash))
        |> assign(:log_event, le)
      else
        start_task(uuid: id, partitions_range: [dminus1, dplus1], source: assigns.source)
        assign(socket, Map.delete(assigns, :flash))
      end

    socket = assign_defaults(socket, assigns)

    {:ok, socket}
  end

  @impl true
  def render(%{log_event: %LE{body: %{metadata: metadata}} = le} = assigns) do
    tz =
      if assigns.team_user,
        do: assigns.team_user.preferences.timezone,
        else: assigns.user.preferences.timezone

    timestamp = Timex.from_unix(le.body.timestamp, :microsecond)
    local_timestamp = Timex.to_datetime(timestamp, tz)

    LogView.render("log_event_body.html",
      source: le.source,
      metadata: metadata,
      fmt_metadata: BqSchema.encode_metadata(metadata),
      message: le.body.message,
      id: le.id,
      timestamp: timestamp,
      local_timezone: tz,
      local_timestamp: local_timestamp
    )
  end

  @impl true
  def render(_assigns) do
    SharedView.render("loader.html")
  end

  defp assign_defaults(socket, assigns) do
    socket
    |> assign(:user, assigns.user)
    |> assign(:team_user, assigns.team_user)
    |> assign(:source, assigns.source)
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
