defmodule LogflareWeb.LogEventLive do
  @moduledoc """
  Handles all user interactions with a single event
  """

  use LogflareWeb, :live_view

  require Logger

  alias Logflare.Logs.LogEvents
  alias Logflare.Sources

  on_mount LogflareWeb.AuthLive

  def mount(%{"source_id" => source_id} = params, _session, socket) do
    source =
      source_id |> String.to_integer() |> Sources.Cache.get_by_id()

    timestamp =
      if ts = Map.get(params, "timestamp") do
        {:ok, dt, _} = DateTime.from_iso8601(ts)
        dt
      end

    lql = params["lql"] || ""
    is_tailing = params["tailing?"] == "true"

    opts =
      [
        source: source,
        user: socket.assigns.user,
        lql: lql
      ]
      |> maybe_put_timestamp(timestamp)

    log_event =
      case LogEvents.get_event_with_fallback(source.token, params["uuid"], opts) do
        {:ok, le} -> le
        {:error, _} -> nil
      end

    socket =
      socket
      |> assign(:source, source)
      |> assign(:log_event, log_event)
      |> assign(:origin, params["origin"])
      |> assign(:log_event_id, params["uuid"])
      |> assign(:lql, lql)
      |> assign(:tailing?, is_tailing)
      |> assign(:timestamp, timestamp)

    {:ok, socket}
  end

  def render(assigns) do
    LogflareWeb.LogView.render("log_event.html", assigns)
  end

  def handle_info({:put_flash, type, msg}, socket) do
    {:noreply, socket |> put_flash(type, msg)}
  end

  @spec maybe_put_timestamp(Keyword.t(), DateTime.t() | nil) :: Keyword.t()
  defp maybe_put_timestamp(opts, nil), do: opts
  defp maybe_put_timestamp(opts, timestamp), do: Keyword.put(opts, :timestamp, timestamp)
end
