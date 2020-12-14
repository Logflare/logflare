defmodule LogflareWeb.Search.UserPreferencesComponent do
  @moduledoc """
  Responsible for user preference mangament
  """
  alias Logflare.Users
  alias Logflare.Users.UserPreferences
  alias LogflareWeb.SearchView
  use LogflareWeb, :live_component
  import LogflareWeb.LiveViewUtils
  @default_timezone "Etc/UTC"

  @impl true
  def mount(socket) do
    tzos = for t <- Timex.timezones(), do: {String.to_atom(t), t}

    socket =
      socket
      |> assign(:timezones_form_options, tzos)
      |> assign(:title, "User preferences")

    {:ok, socket}
  end

  @impl true
  def update(%{team_user: user} = assigns, socket) when user do
    user_prefs =
      if user.preferences do
        Users.change_user_preferences(user.preferences)
      else
        Users.change_user_preferences(
          %UserPreferences{},
          %{timezone: @default_timezone}
        )
      end

    {:ok,
     socket
     |> assign(assigns)
     |> assign(:user_or_team_user, user)
     |> assign(:user_type, :team_user)
     |> assign(
       :user_preferences,
       user_prefs
     )}
  end

  @impl true
  def update(%{user: user} = assigns, socket) do
    IO.inspect("hit")

    user_prefs =
      if user.preferences do
        Users.change_user_preferences(user.preferences)
      else
        Users.change_user_preferences(
          %UserPreferences{},
          %{timezone: @default_timezone}
        )
      end

    {:ok,
     socket
     |> assign(assigns)
     |> assign(:user_type, :user)
     |> assign(:user_or_team_user, user)
     |> assign(
       :user_preferences,
       user_prefs
     )}
  end

  @impl true
  def handle_event(
        "update-preferences",
        %{"user_preferences" => user_preferences} = meta,
        socket
      ) do
    socket =
      socket.assigns.user_or_team_user
      |> Users.update_user_with_preferences(%{"preferences" => user_preferences})
      |> case do
        {:ok, user} ->
          socket
          |> assign(:user_or_team_user, user)
          |> assign(:user_preferences, Users.change_user_preferences(user.preferences))
          |> assign_notifications(:warning, "Search saved!")
          |> put_flash(:success, "Local timezone updated to #{user.preferences.timezone}")

        {:error, _error} ->
          socket
          |> put_flash(:error, "Something went wrong")
      end

    # |> push_patch(to: socket.assigns.return_to)

    {:noreply, socket}
  end

  @impl true
  def render(assigns) do
    SearchView.render("user_prefs_component.html", assigns)
  end
end
