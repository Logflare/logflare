defmodule LogflareWeb.SearchLive.TimezoneComponent do
  use LogflareWeb, :html
  use Phoenix.Component

  alias Logflare.DateTimeUtils

  attr :search_timezone, :string, required: true
  attr :user_preferences, :map, required: true

  def timezone(assigns) do
    ~H"""
    <.form :let={f} for={build_form(assigns)} phx-change="results-action-change" id="results-actions">
      <label class="sr-only" for="display_timezone">Display Timezone</label>
      <div class="input-group input-group-sm ">
        <div class="input-group-prepend tw-grow">
          <div class="input-group-text tw-py-0 tw-bg-transparent tw-text-black tw-text-xs">display timezone</div>
        </div>
        <%= select(f, :search_timezone, build_timezones_select_form_options(), selected: @search_timezone, class: "form-control form-control-sm tw-w-64 tw-text-xs") %>
        <button type="button" class="btn btn-link tw-text-xs tw-py-0" phx-click="results-action-change" phx-value-search_timezone="Etc/UTC">UTC</button>
        <span :if={show_checkbox?(assigns)} class="tw-relative tw-align-text-bottom">
          <%= checkbox(f, :remember_timezone, class: "tw-align-middle") %>
          <%= label(f, :remember_timezone, "Remember", class: "tw-text-xs tw-my-0 tw-leading-3") %>
        </span>
      </div>
    </.form>
    """
  end

  def build_form(%{search_timezone: search_timezone, user_preferences: preferences}) do
    user_timezone = user_timezone(preferences)

    %{
      "search_timezone" => search_timezone,
      "remember_timezone" => search_timezone == user_timezone
    }
  end

  def show_checkbox?(%{user_preferences: preferences, search_timezone: search_timezone}) do
    search_timezone != user_timezone(preferences)
  end

  defp user_timezone(preferences), do: preferences && Map.get(preferences, :timezone)

  defp build_timezones_select_form_options() do
    Timex.timezones()
    |> Enum.map(&[offset: Timex.Timezone.get(&1).offset_utc, t: &1])
    |> Enum.sort_by(& &1[:offset])
    |> Enum.map(fn [offset: offset, t: t] ->
      hoursstring = DateTimeUtils.humanize_timezone_offset(offset)

      {String.to_atom("#{t} (#{hoursstring})"), t}
    end)
  end
end
