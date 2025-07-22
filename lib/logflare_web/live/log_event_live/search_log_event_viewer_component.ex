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
    params = event_params(assigns)

    socket =
      socket
      |> assign_defaults(assigns)
      |> start_async(:load, fn ->
        load_event(params)
      end)

    {:ok, socket}
  end

  def update(%{log_event: log_event_result} = assigns, socket) do
    socket =
      assign(socket, :log_event, log_event_result)
      |> assign_defaults(assigns)

    {:ok, socket}
  end

  def update(assigns, socket) do
    %{"log-event-id" => id, "log-event-timestamp" => timestamp} = assigns.params
    d = String.to_integer(timestamp) |> DateTime.from_unix!(:microsecond) |> DateTime.to_date()

    params =
      event_params(assigns)
      |> Map.merge(%{log_event_id: id, timestamp: d})
      |> Map.put(:lql, assigns.params["lql"] || "")

    socket =
      socket
      |> assign_defaults(assigns)
      |> start_async(:load, fn ->
        load_event(params)
      end)

    {:ok, socket}
  end

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

  def load_event(%{log_event_id: log_id, source: source} = params) do
    range = get_partitions_range(params)
    LogEvents.fetch_event_by_id(source.token, log_id, partitions_range: range, lql: params[:lql])
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
    timestamp = socket.assigns[:timestamp] || assigns[:timestamp]
    lql = socket.assigns[:lql] || assigns[:lql] || ""

    socket
    |> assign(:user, user)
    |> assign(:team_user, team_user)
    |> assign(:source, source)
    |> assign(:timestamp, timestamp)
    |> assign(:lql, lql)
  end

  # timestamp is explicitly set, query around the range
  defp get_partitions_range(%{timestamp: ts}) do
    [Timex.shift(ts, hours: -1), Timex.shift(ts, hours: 1)]
  end

  # no timestamp is set, fallback to ttl
  # to avoid this clause, parent should set the timestamp
  defp get_partitions_range(%{source: source, user: user}) do
    d = Date.utc_today()
    plan = Billing.Cache.get_plan_by_user(user)
    ttl = Sources.source_ttl_to_days(source, plan)

    [Timex.shift(d, days: -min(ttl, 7)), Timex.shift(d, days: 1)]
  end

  defp event_params(assigns) do
    assigns |> Map.take([:user, :source, :log_event_id, :timestamp])
  end
end
