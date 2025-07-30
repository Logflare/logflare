defmodule LogflareWeb.Search.UserPreferencesComponent do
  @moduledoc """
  Responsible for user preference mangament
  """
  alias Logflare.Users
  alias Logflare.Users.UserPreferences
  alias Logflare.TeamUsers.TeamUser
  alias Logflare.DateTimeUtils
  alias LogflareWeb.SearchView
  use LogflareWeb, :live_component
  @default_timezone "Etc/UTC"

  @impl true
  def mount(socket) do
    socket =
      socket
      |> assign(:timezones_form_options, build_timezones_select_form_options())
      |> assign(:title, "User preferences")

    {:ok, socket}
  end

  @impl true
  def update(%{team_user: %TeamUser{} = user} = assigns, socket) do
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
        %{"user_preferences" => user_preferences},
        socket
      ) do
    tz = Map.get(user_preferences, "timezone")

    socket =
      socket.assigns.user_or_team_user
      |> Users.update_user_with_preferences(%{"preferences" => user_preferences})
      |> case do
        {:ok, user} ->
          socket
          |> assign(:user_or_team_user, user)
          |> assign(:user_preferences, Users.change_user_preferences(user.preferences))
          |> put_flash(:info, "Local timezone updated to #{user.preferences.timezone}")

        {:error, _error} ->
          socket
          |> put_flash(:error, "Something went wrong")
      end
      |> then(fn socket ->
        uri = URI.parse(socket.assigns.return_to)
        query = URI.decode_query(uri.query) |> Map.merge(%{"tz" => tz})
        updated_uri = %{uri | query: URI.encode_query(query)} |> URI.to_string()

        socket
        |> push_navigate(to: updated_uri)
      end)

    {:noreply, socket}
  end

  @impl true
  def render(assigns) do
    SearchView.render("user_prefs_component.html", assigns)
  end

  def build_timezones_select_form_options() do
    Timex.timezones()
    |> Enum.map(&[offset: Timex.Timezone.get(&1).offset_utc, t: &1])
    |> Enum.sort_by(& &1[:offset])
    |> Enum.map(fn [offset: offset, t: t] ->
      hoursstring = DateTimeUtils.humanize_timezone_offset(offset)

      {String.to_atom("#{t} (#{hoursstring})"), t}
    end)
  end
end
