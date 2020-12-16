defmodule LogflareWeb.LogEventLive.Show do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use LogflareWeb, :live_view

  # alias LogflareWeb.LogView
  alias Logflare.Logs.LogEvents
  alias Logflare.LogEvent, as: LE
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.Sources

  use LogflareWeb.ModalsLVHelpers

  require Logger

  def mount(%{"source_id" => source_id} = params, _session, socket) do
    source = Sources.get_source_for_lv_param(source_id)
    token = source.token

    {path, log_id} = cache_key = params_to_cache_key(params)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:origin, params["origin"])
      |> assign(:id_param, log_id)

    le = LogEvents.Cache.get!(token, cache_key)

    socket =
      cond do
        socket.assigns[:log_event] ->
          assign(socket, :loading, false)

        match?(%LE{}, le) ->
          assign_log_event(socket, le)

        is_nil(le) and connected?(socket) ->
          d = Date.utc_today()
          dminus3 = Timex.shift(d, days: -3)
          dplus1 = Timex.shift(d, days: 1)

          task_fn = fn ->
            {cache_key,
             case path do
               "uuid" ->
                 LogEvents.fetch_event_by_id(token, log_id, partitions_range: [dminus3, dplus1])

               "metadata." <> _ ->
                 LogEvents.fetch_event_by_path(token, path, log_id)
             end}
          end

          Task.async(task_fn)

          assign(socket, :loading, true)

        true ->
          assign(socket, :loading, true)
      end

    socket =
      if connected?(socket) do
        user_timezone =
          socket
          |> get_connect_params()
          |> Map.get("user_timezone")

        assign(socket, :user_local_timezone, user_timezone)
      else
        socket
      end

    {:ok, socket}
  end

  def handle_params(_params, _uri, socket) do
    socket =
      socket
      |> assign_new(:message, fn -> nil end)
      |> assign_new(:id, fn -> nil end)
      |> assign_new(:fmt_metadata, fn -> nil end)
      |> assign_new(:metadata, fn -> nil end)
      |> assign_new(:timestamp, fn -> nil end)
      |> assign_new(:error, fn -> nil end)
      |> assign_new(:loading, fn -> false end)
      |> assign_new(:user_local_timezone, fn -> false end)

    {:noreply, socket}
  end

  def render(assigns) do
    LogflareWeb.LogView.render("log_event.html", assigns)
  end

  def handle_info({_ref, {cache_key, msg}}, socket) do
    token = socket.assigns.source.token

    socket =
      case msg do
        nil ->
          assign(socket, :loading, false)

        bq_row when is_map(bq_row) ->
          le = LE.make_from_db(bq_row, %{source: socket.assigns.source})

          LogEvents.Cache.put(
            token,
            cache_key,
            le
          )

          assign_log_event(socket, le)

        {:error, error} ->
          Logger.error("Log event event query error: #{inspect(error)}")

          socket
          |> assign(:loading, false)
          |> assign(:error, error)
      end

    {:noreply, socket}
  end

  # handling task DOWN message
  def handle_info({:DOWN, _, _, _, _}, socket), do: {:noreply, socket}

  @spec assign_log_event(Phoenix.LiveView.Socket.t(), LE.t()) :: Phoenix.LiveView.Socket.t()
  defp assign_log_event(socket, %LE{body: %{metadata: m, message: msg, timestamp: ts}} = le) do
    socket
    |> assign(:metadata, m)
    |> assign(:fmt_metadata, BqSchema.encode_metadata(m))
    |> assign(:message, msg)
    |> assign(:id, le.id)
    |> assign(:timestamp, Timex.from_unix(ts, :microsecond))
    |> assign(:loading, false)
  end

  @spec params_to_cache_key(map()) :: {String.t(), String.t()}
  defp params_to_cache_key(%{"uuid" => id}) do
    {"uuid", id}
  end

  defp params_to_cache_key(%{"path" => path, "value" => value}) do
    {path, value}
  end
end
