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

  require Logger

  def mount(%{"source_id" => source_id} = params, _session, socket) do
    source = Sources.get_source_for_lv_param(source_id)
    token = source.token

    {path, log_id} = cache_key = params_to_cache_key(params)

    socket =
      socket
      |> assign(:source, source)
      |> assign(:path, path)
      |> assign(:origin, params["origin"])
      |> assign(:id_param, log_id)

    le = LogEvents.Cache.get!(token, cache_key)

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

  @spec params_to_cache_key(map()) :: {String.t(), String.t()}
  defp params_to_cache_key(%{"uuid" => id}) do
    {"uuid", id}
  end

  defp params_to_cache_key(%{"path" => path, "value" => value}) do
    {path, value}
  end
end
