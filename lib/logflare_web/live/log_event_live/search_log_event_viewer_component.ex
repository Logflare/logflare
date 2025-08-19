defmodule LogflareWeb.Search.LogEventViewerComponent do
  @moduledoc false
  use LogflareWeb, :live_component

  alias Logflare.Billing
  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.LogEvents
  alias Logflare.Sources
  alias LogflareWeb.Helpers.BqSchema
  alias LogflareWeb.LogView
  alias LogflareWeb.SharedView

  require Logger

  @impl true
  def update(_assigns, %{assigns: %{error: {:error, :not_found}}} = socket) do
    {:ok, socket}
  end

  def update(%{log_event: %LE{} = log_event_result} = assigns, socket) do
    socket =
      assign(socket, :log_event, log_event_result)
      |> assign_defaults(assigns)
      |> assign(:lql, assigns.params["lql"])

    {:ok, socket}
  end

  def update(assigns, socket) do
    %{"log-event-id" => id, "log-event-timestamp" => timestamp} = assigns.params

    d =
      if is_binary(timestamp),
        do: String.to_integer(timestamp) |> DateTime.from_unix!(:microsecond),
        else: timestamp

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

  @impl true
  def handle_async(:load, {:ok, %Logflare.LogEvent{} = log_event}, socket) do
    {:noreply, assign(socket, :log_event, log_event)}
  end

  @impl true
  def handle_async(:load, {:ok, %{} = bq_row}, socket) do
    le = LE.make_from_db(bq_row, %{source: socket.assigns.source})

    LogEvents.Cache.put(
      socket.assigns.source.token,
      le.id,
      le
    )

    handle_async(:load, {:ok, le}, socket)
  end

  @impl true
  def handle_async(:load, {:ok, {:error, :not_found}}, socket) do
    {:noreply, socket |> assign(:error, {:error, :not_found})}
  end

  @impl true
  def handle_async(:load, {:ok, {:error, raw_err}}, socket) do
    Logger.error("Error loading log event: #{Logflare.Utils.stringify(raw_err)}")
    err = "Oops, something went wrong!"
    send(self(), {:put_flash, :error, err})
    {:noreply, socket}
  end

  def load_event(%{log_event_id: log_id, source: source} = params) do
    range = get_partitions_range(params)

    case LogEvents.Cache.get(source.token, log_id) do
      {:ok, %LE{} = le} ->
        le

      _ ->
        LogEvents.Cache.fetch_event_by_id(source.token, log_id,
          partitions_range: range,
          lql: params.lql
        )
    end
  end

  @impl true
  def render(%{error: {:error, :not_found}} = assigns) do
    ~H"""
    <div class="">
      <h4>Log Event Not Found</h4>
      <p>The requested log event could not be found. It may have been deleted or the ID is incorrect.</p>
    </div>
    """
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
      lql: assigns.lql,
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
    |> assign(:error, nil)
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
