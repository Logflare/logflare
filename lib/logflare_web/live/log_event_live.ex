defmodule LogflareWeb.LogEventLive do
  @moduledoc """
  Handles all user interactions with the source logs search
  """
  use LogflareWeb, :live_view

  # alias LogflareWeb.LogView
  alias Logflare.Logs.LogEvents
  alias Logflare.Sources
  alias Logflare.Users
  alias Logflare.TeamUsers

  require Logger

  def mount(%{"source_id" => source_id} = params, session, socket) do
    source_id = String.to_integer(source_id)
    ts = Map.get(params, "timestamp")
    source = Sources.get(source_id)
    token = source.token

    team_user =
      if team_user_id = session["team_user_id"] do
        TeamUsers.get_team_user_by(id: team_user_id)
      else
        nil
      end

    user =
      if user_id = session["user_id"] do
        Users.get_by(id: user_id)
      else
        nil
      end

    socket =
      socket
      |> assign(:source, source)
      |> assign(:user, user)
      |> assign(:team_user, team_user)
      |> assign(:log_event, nil)
      |> assign(:origin, params["origin"])
      |> assign(:log_event_id, params["uuid"])
      |> case do
        socket when ts != nil ->
          {:ok, dt, _} = DateTime.from_iso8601(ts)
          assign(socket, :timestamp, dt)

        socket ->
          socket
      end
      |> assign(:log_event, LogEvents.Cache.get!(token, {"uuid", params["uuid"]}))

    {:ok, socket}
  end

  def render(assigns) do
    LogflareWeb.LogView.render("log_event.html", assigns)
  end

  def handle_info({:put_flash, type, msg}, socket) do
    {:noreply, socket |> put_flash(type, msg)}
  end
end
