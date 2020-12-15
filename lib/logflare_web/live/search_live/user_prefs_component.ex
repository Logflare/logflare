defmodule LogflareWeb.Search.UserPreferencesComponent do
  @moduledoc """
  Responsible for user preference mangament
  """
  alias Logflare.Users
  alias Logflare.Users.UserPreferences
  alias Logflare.TeamUsers.TeamUser
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
    socket =
      socket.assigns.user_or_team_user
      |> Users.update_user_with_preferences(%{"preferences" => user_preferences})
      |> case do
        {:ok, user} ->
          socket
          |> assign(:user_or_team_user, user)
          |> assign(:user_preferences, Users.change_user_preferences(user.preferences))
          |> put_flash(:success, "Local timezone updated to #{user.preferences.timezone}")

        {:error, _error} ->
          socket
          |> put_flash(:error, "Something went wrong")
      end
      |> push_patch(to: socket.assigns.return_to)

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
      {hours, minutes, _, _} =
        offset
        |> Timex.Duration.from_seconds()
        |> Timex.Duration.to_clock()

      hours =
        case "#{hours}" do
          "-" <> rest when abs(hours) < 10 -> "-0" <> rest
          rest when abs(hours) < 10 -> "+0" <> rest
          x when hours >= 10 -> "+" <> x
          x when hours <= 10 -> x
        end

      minutes_prefix =
        if abs(minutes) < 10 do
          "0"
        else
          ""
        end

      minutes = "#{minutes_prefix}#{minutes}"

      hoursstring = "(#{hours}:#{minutes})"

      {String.to_atom("#{t} #{hoursstring}"), t}
    end)
  end
end
