defmodule LogflareWeb.Search.LogEventViewerComponent do
  @moduledoc false

  use LogflareWeb, :live_component

  require Logger

  alias Logflare.LogEvent, as: LE
  alias Logflare.Logs.LogEvents
  alias LogflareWeb.Helpers.BqSchema
  alias LogflareWeb.LogView
  alias LogflareWeb.SharedView

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
  def handle_async(:load, {:ok, %LE{} = log_event}, socket) do
    {:noreply, assign(socket, :log_event, log_event)}
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
    opts =
      [
        source: source,
        user: params[:user],
        lql: params[:lql] || ""
      ]
      |> maybe_put_timestamp(params[:timestamp])

    case LogEvents.get_event_with_fallback(source.token, log_id, opts) do
      {:ok, le} -> le
      error -> error
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

  defp event_params(assigns) do
    assigns |> Map.take([:user, :source, :log_event_id, :timestamp])
  end

  @spec maybe_put_timestamp(Keyword.t(), DateTime.t() | nil) :: Keyword.t()
  defp maybe_put_timestamp(opts, nil) when is_list(opts), do: opts

  defp maybe_put_timestamp(opts, timestamp) when is_list(opts),
    do: Keyword.put(opts, :timestamp, DateTime.truncate(timestamp, :second))
end
