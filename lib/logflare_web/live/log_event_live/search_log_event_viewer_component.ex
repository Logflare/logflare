defmodule LogflareWeb.Search.LogEventViewerComponent do
  @moduledoc false
  use LogflareWeb, :live_component
  alias LogflareWeb.LogView
  require Logger
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.Logs.LogEvents
  alias Logflare.LogEvent, as: LE
  alias LogflareWeb.SharedView
  alias Logflare.Billing
  alias Logflare.Sources

  @impl true
  def update(%{log_event: nil} = assigns, socket) do
    socket =
      socket
      |> assign_defaults(assigns)
      |> start_async(:load, fn ->
        load_event(assigns)
      end)

    {:ok, socket}
  end

  def update(%{log_event: log_event_result} = assigns, socket) do
    socket =
      assign(socket, :log_event, log_event_result)
      |> assign_defaults(assigns)

    {:ok, socket}
  end

  # @impl true
  # def update(assigns, socket) do
  #   %{"log-event-id" => id, "log-event-timestamp" => timestamp} = assigns.params

  #   d = String.to_integer(timestamp) |> Timex.from_unix(:microsecond) |> Timex.to_date()
  #   dminus1 = Timex.shift(d, days: -1)
  #   dplus1 = Timex.shift(d, days: +1)

  #   token = assigns.source.token
  #   le = LogEvents.Cache.get!(token, params_to_cache_key(%{uuid: id}))

  #   socket =
  #     if le do
  #       socket
  #       |> assign(:log_event, le)
  #     else
  #       start_task(uuid: id, partitions_range: [dminus1, dplus1], source: assigns.source)
  #       socket
  #     end

  #   socket = assign_defaults(socket, assigns)

  #   {:ok, socket}
  # end

  def handle_async(:load, {:ok, %{} = bq_row}, socket) do
    le = LE.make_from_db(bq_row, %{source: socket.assigns.source})

    LogEvents.Cache.put(
      socket.assigns.source.token,
      {"uuid", le.id},
      le
    )

    {:noreply, assign(socket, :log_event, le)}
  end

  def handle_async(:load, {:ok, {:error, :not_found}}, socket) do
    [from, to] = socket.assigns.partitions_range

    err =
      "Log event with id #{socket.assigns.log_event_id} between #{from} and #{to} was not found"

    Logger.warning(err)
    send(self(), {:put_flash, :error, err})

    {:noreply, socket}
  end

  def handle_async(:load, {:ok, {:error, raw_err}}, socket) do
    Logger.error("Error loading log event: #{Logflare.Utils.stringify(raw_err)}")
    err = "Oops, something went wrong! #{Logflare.Utils.stringify(raw_err)}"
    send(self(), {:put_flash, :error, err})
    {:noreply, socket}
  end

  def load_event(%{log_event_id: log_id, source: source} = assigns) do
    range = get_partitions_range(assigns)
    LogEvents.fetch_event_by_id(source.token, log_id, partitions_range: range)
  end

  @impl true
  def render(%{source: source, log_event: %LE{body: body} = le} = assigns) do
    tz =
      if assigns.team_user,
        do: Map.get(assigns.team_user.preferences || %{}, :timezone, "Etc/UTC"),
        else: Map.get(assigns.user.preferences || %{}, :timezone, "Etc/UTC")

    timestamp = Timex.from_unix(body["timestamp"], :microsecond)
    local_timestamp = Timex.to_datetime(timestamp, tz)

    LogView.render("log_event_body.html",
      source: source,
      body: body,
      fmt_body: BqSchema.encode_metadata(body),
      message: body["event_message"],
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
    user = socket.assigns[:user] || assigns[:user]
    team_user = socket.assigns[:team_user] || assigns[:team_user]
    source = socket.assigns[:source] || assigns[:source]

    socket
    |> assign(:user, user)
    |> assign(:team_user, team_user)
    |> assign(:source, source)
  end

  defp get_partitions_range(%{source: source, user: user} = assigns) do
    d = Date.utc_today()
    plan = Billing.Cache.get_plan_by_user(user)
    ttl = Sources.source_ttl_to_days(source, plan)

    cond do
      iso_timestamp = Map.get(assigns, :timestamp) ->
        # timestamp is explicitly set, query around the range
        {:ok, ts} = DateTime.from_iso8601(iso_timestamp)
        [ts, Timex.shift(d, days: 1)]

      # no timestamp is set, fallback to ttl
      # to avoid this clause, parent should set the timestamp
      ttl <= 7 ->
        [Timex.shift(d, days: -ttl), Timex.shift(d, days: 1)]

      true ->
        [Timex.shift(d, days: -90), Timex.shift(d, days: 1)]
    end
  end

  # @spec params_to_cache_key(map()) :: {String.t(), String.t()}
  # defp params_to_cache_key(%{uuid: id}) do
  #   {"uuid", id}
  # end

  # defp params_to_cache_key(%{path: path, value: value}) do
  #   {path, value}
  # end

  # defp start_task(params) do
  #   params = Enum.into(params, %{})
  #   source = params.source

  #   pid = self()

  #   Task.start(fn ->
  #     case params do
  #       %{uuid: id} ->
  #         LogEvents.fetch_event_by_id(source.token, id, partitions_range: params.partitions_range)

  #       %{id: id} ->
  #         LogEvents.fetch_event_by_id(source.token, id, partitions_range: params.partitions_range)

  #       %{path: "uuid", value: id} ->
  #         LogEvents.fetch_event_by_id(source.token, id, partitions_range: params.partitions_range)

  #       %{path: path, value: value} ->
  #         LogEvents.fetch_event_by_path(source.token, path, value)
  #     end
  #     |> case do
  #       %{} = bq_row ->
  #         dbg("make from db running")

  #         le = LE.make_from_db(bq_row, %{source: source}) |> dbg()

  #         LogEvents.Cache.put(
  #           source.token,
  #           params_to_cache_key(params) |> dbg(),
  #           le
  #         )

  #         send_update(pid, __MODULE__, log_event: le, id: :log_event_viewer) |> dbg()

  #       {:error, %Tesla.Env{} = err} ->
  #         Logger.warning("Tesla Error when fetching log event | status=#{err.status}",
  #           tesla_response: inspect(err)
  #         )

  #       {:error, error} ->
  #         error =
  #           case error do
  #             :not_found ->

  #             e ->
  #     end
  #   end)
  # end
end
