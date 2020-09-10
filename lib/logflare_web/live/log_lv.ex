defmodule LogflareWeb.Logs.LV do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use Phoenix.LiveView, layout: {LogflareWeb.LayoutView, "live.html"}

  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Lql
  alias LogflareWeb.LogView
  alias Logflare.Logs.SearchQueryExecutor
  alias Logflare.Logs.LogEvents
  alias Logflare.LogEvent, as: LE
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.Sources

  use LogflareWeb.LiveViewUtils
  use LogflareWeb.ModalsLVHelpers

  require Logger

  def mount(%{"source_id" => source_id, "uuid" => log_id}, _session, socket) do
    source = Sources.get_source_for_lv_param(source_id)
    socket = assign(socket, :source, source)
    socket = assign(socket, :id_param, log_id)

    le = LogEvents.Cache.get!(source.token, "uuid:#{log_id}")

    socket =
      cond do
        socket.assigns[:log_event] ->
          assign(socket, :loading, false)

        match?(%LE{}, le) ->
          assign_log_event(socket, le)

        is_nil(le) and connected?(socket) ->
          Task.async(fn ->
            {:uuid, LogEvents.fetch_event_by_id(source.token, log_id)}
          end)

          assign(socket, :loading, true)

        true ->
          assign(socket, :loading, true)
      end

    {:ok, socket}
  end

  @vercel_id_path "metadata.id"
  def mount(%{"source_id" => source_id, "vercel_id" => log_id}, _session, socket) do
    source = Sources.get_source_for_lv_param(source_id)
    socket = assign(socket, :source, source)
    socket = assign(socket, :id_param, log_id)
    le = LogEvents.Cache.get!(source.token, "vercel:#{log_id}")

    socket =
      cond do
        socket.assigns[:log_event] ->
          socket

        match?(%LE{}, le) ->
          assign_log_event(socket, le)

        connected?(socket) ->
          Task.async(fn ->
            {:vercel, LogEvents.fetch_event_by_path(source.token, "metadata.id", log_id)}
          end)

          assign(socket, :loading, true)

        true ->
          assign(socket, :loading, true)
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
      |> assign_new(:log_event, fn -> nil end)
      |> assign_new(:loading, fn -> false end)

    {:noreply, socket}
  end

  def render(assigns) do
    LogView.render("log_event.html", assigns)
  end

  def handle_info({_ref, {origin, msg}}, socket) do
    socket =
      case msg do
        nil ->
          assign(socket, :loading, false)

        bq_row when is_map(bq_row) ->
          le = LE.make_from_db(bq_row, %{source: socket.assigns.source})

          case origin do
            :vercel ->
              LogEvents.Cache.put(
                socket.assigns.source.token,
                "vercel:#{le.body.metadata.id}",
                le
              )

            :uuid ->
              LogEvents.Cache.put(
                socket.assigns.source.token,
                "uuid:#{le.id}",
                le
              )
          end

          assign_log_event(socket, le)

        {:error, error} ->
          assign(socket, :error, error)
      end

    {:noreply, socket}
  end

  def handle_info({:DOWN, _, _, _, _}, socket), do: {:noreply, socket}

  def assign_log_event(socket, %LE{} = le) do
    socket
    |> assign(:metadata, le.body.metadata)
    |> assign(:fmt_metadata, BqSchema.encode_metadata(le.body.metadata))
    |> assign(:message, le.body.message)
    |> assign(:id, le.id)
    |> assign(:timestamp, Timex.from_unix(le.body.timestamp, :microsecond))
    # |> assign(:error, nil)
    |> assign(:loading, false)
  end

  # handling task DOWN message
end
