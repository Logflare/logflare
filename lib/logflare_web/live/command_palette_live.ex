defmodule LogflareWeb.CommandPaletteLive do
  @moduledoc """
  Global CMD/CTRL+K command palette.

  Owns no visible UI in Elixir — the JS hook `CommandPalette`
  (assets/js/command_palette_hook.js) builds the modal in the DOM, listens for
  the keyboard shortcut, and asks this LiveView for the user's source list on
  first open.
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
    {:ok,
     socket
     |> assign(:current_email, session["current_email"])
     |> assign(:team_id, session["last_switched_team_id"])}
  end

  @impl true
  def render(assigns) do
    ~H"""
    <div id="command-palette" phx-hook="CommandPalette" data-team-id={@team_id || ""}></div>
    """
  end

  @impl true
  def handle_event("fetch_sources", _params, socket) do
    sources =
      case socket.assigns.current_email do
        email when is_binary(email) ->
          case Users.get_by(email: email) do
            nil -> []
            user -> list_payloads(user)
          end

        _ ->
          []
      end

    {:reply, %{sources: sources}, socket}
  end

  defp list_payloads(user) do
    [user_id: user.id, system_source: false]
    |> Sources.list_sources()
    |> Enum.map(&to_payload/1)
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
end
