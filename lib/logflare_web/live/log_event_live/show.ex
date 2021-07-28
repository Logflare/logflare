defmodule LogflareWeb.LogEventLive.Show do
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
    source = Sources.get_source_for_lv_param(source_id)

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

    {path, log_id} = cache_key = params_to_cache_key(params)


    socket =
      socket
      |> assign(:source, source)
      |> assign(:user, user)
      |> assign(:team_user, team_user)
      |> assign(:path, path)
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
