defmodule LogflareWeb.LogEventLive.Show do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use LogflareWeb, :live_view

  # alias LogflareWeb.LogView
  alias Logflare.Logs.LogEvents
  alias Logflare.Sources

  require Logger

  def mount(%{"source_id" => source_id} = params, _session, socket) do
    source = Sources.get_source_for_lv_param(source_id)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:origin, params["origin"])

    le =
      case params do
        %{"uuid" => uuid} ->
          LogEvents.get_log_event!(uuid)

        %{"path" => "metadata.id", "value" => value} ->
          LogEvents.get_log_event_by_metadata_for_source(%{"id" => value}, source.id)
      end

    socket =
      if le do
        assign(socket, :log_event, le)
      else
        socket
      end

    {:ok, socket}
  end

  def render(assigns) do
    LogflareWeb.LogView.render("log_event.html", assigns)
  end
end
