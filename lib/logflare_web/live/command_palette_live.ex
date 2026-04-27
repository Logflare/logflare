defmodule LogflareWeb.CommandPaletteLive do
  @moduledoc """
  Global CMD/CTRL+K command palette.

  Mounted once in the root layout via `live_render/3` so it is present on every
  authenticated page, including controller-rendered ones. Owns no visible UI in
  Elixir — the JS hook `CommandPalette` (assets/js/command_palette_hook.js) builds
  the modal in the DOM, listens for the keyboard shortcut, and asks this LiveView
  for the user's source list on first open.
  """
  use LogflareWeb, {:live_view, layout: false}

  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Users

  @type source_payload :: %{
          id: pos_integer(),
          name: String.t(),
          favorite: boolean(),
          service_name: String.t() | nil
        }

  @impl true
  def mount(_params, session, socket) do
    user = socket.assigns[:user] || lookup_user(session["current_email"])
    team_id = socket.assigns[:team] && socket.assigns.team.id

    {:ok,
     socket
     |> assign(:user, user)
     |> assign(:team_id, team_id || session["last_switched_team_id"])}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div id="command-palette" phx-hook="CommandPalette" data-team-id={@team_id || ""}></div>
    """
  end

  @impl true
  def handle_event("fetch_sources", _params, %{assigns: %{user: nil}} = socket) do
    {:reply, %{sources: []}, socket}
  end

  def handle_event("fetch_sources", _params, socket) do
    sources =
      socket.assigns.user
      |> Sources.list_sources_by_user()
      |> Enum.reject(& &1.system_source)
      |> Enum.map(&to_payload/1)

    {:reply, %{sources: sources}, socket}
  end

  @spec to_payload(Source.t()) :: source_payload()
  defp to_payload(source) do
    %{
      id: source.id,
      name: source.name,
      favorite: source.favorite || false,
      service_name: source.service_name
    }
  end

  defp lookup_user(nil), do: nil

  defp lookup_user(email) when is_binary(email) do
    Users.get_by(email: email)
  end
end
