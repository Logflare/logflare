defmodule LogflareWeb.SearchLive.DisplayTimezoneComponent do
  use LogflareWeb, :html

  def display_timezone(assigns) do
    ~H"""
    <.form :let={f} for={build_form(assigns)} phx-change="results-action-change" id="results-actions">
      <label class="sr-only" for="display_timezone">Display Timezone</label>
      <div class="input-group input-group-sm ">
        <div class="input-group-prepend tw-grow">
          <div class="input-group-text tw-py-0 tw-bg-transparent tw-text-black tw-text-xs">display timezone</div>
        </div>
        <%= select(f, :display_timezone, LogflareWeb.Search.UserPreferencesComponent.build_timezones_select_form_options(), selected: @display_timezone, class: "form-control form-control-sm tw-w-64 tw-text-xs") %>
        <button type="button" class="btn btn-link tw-text-xs tw-py-0" phx-click="results-action-change" phx-value-display_timezone="Etc/UTC">UTC</button>
        <span :if={show_checkbox?(assigns)} class="tw-relative tw-align-text-bottom">
          <%= checkbox(f, :remember_timezone, class: "tw-align-middle") %>
          <%= label(f, :remember_timezone, "Remember", class: "tw-text-xs tw-my-0 tw-leading-3") %>
        </span>
      </div>
    </.form>
    """
  end

  def build_form(assigns) do
    preferences = assigns.preferences || %{display_timezone: nil}

    %{
      "display_timezone" => assigns.display_timezone,
      "remember_timezone" => preferences.display_timezone != nil
    }
  end

  def show_checkbox?(%{display_timezone: display_timezone, search_timezone: search_timezone}) do
    display_timezone != search_timezone
  end
end
