defmodule LogflareWeb.SearchLive.SubheadComponents do
  @moduledoc """
  Subheader components for logs search page.
  """
  use LogflareWeb, :html

  use Phoenix.Component

  import LogflareWeb.LqlHelpers
  import LogflareWeb.ModalLiveHelpers, only: [modal_link: 1]
  alias Logflare.DateTimeUtils

  attr :user, Logflare.User, required: true
  attr :search_timezone, :string, required: true
  attr :search_op_error, :any, default: nil
  attr :search_op_log_events, :any, default: nil
  attr :search_op_log_aggregates, :any, default: nil

  def subhead_actions(assigns) do
    ~H"""
    <div class="log-settings tw-justify-between tw-mt-2 tw-grow">
      <.timezone user_preferences={@user.preferences} search_timezone={@search_timezone} />
      <ul>
        <li>
          <a href="javascript:Source.scrollOverflowBottom();">
            <span id="scroll-down"><i class="fas fa-chevron-circle-down"></i></span>
            <span class="hide-on-mobile">scroll down</span>
          </a>
        </li>
        <li>
          <.lql_help_modal_link />
        </li>
        <li>
          <.bq_source_schema_modal_link />
        </li>
        <li>
          <span id="search-uri-query" class="pointer-cursor" data-trigger="hover focus" data-delay="0" data-toggle="tooltip" data-html="true" data-placement="top" data-title="<span id=&quot;copy-tooltip&quot;>Copy uri</span>">
            <span>
              <i class="fas fa-copy"></i>
            </span>
            <span class="hide-on-mobile">
              share
            </span>
          </span>
        </li>
        <%= if @search_op_error && is_nil(@search_op_log_events) && is_nil(@search_op_log_aggregates) do %>
          <li>
            <.modal_link component={LogflareWeb.Search.QueryDebugComponent} modal_id={:modal_debug_error_link} title="Query Debugging">
              <i class="fas fa-bug"></i><span class="hide-on-mobile"> debug error </span>
            </.modal_link>
          </li>
        <% else %>
          <li>
            <.modal_link component={LogflareWeb.Search.QueryDebugComponent} modal_id={:modal_debug_log_events_link} title="Query Debugging">
              <i class="fas fa-bug"></i><span class="hide-on-mobile"> events </span>
            </.modal_link>
          </li>
          <li>
            <.modal_link component={LogflareWeb.Search.QueryDebugComponent} modal_id={:modal_debug_log_aggregates_link} title="Query Debugging">
              <i class="fas fa-bug"></i><span class="hide-on-mobile"> aggregate </span>
            </.modal_link>
          </li>
        <% end %>
      </ul>
    </div>
    """
  end

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
        {select(f, :search_timezone, build_timezones_select_form_options(), selected: @search_timezone, class: "form-control form-control-sm tw-w-64 tw-text-xs")}
        <button type="button" class="btn btn-link tw-text-xs tw-py-0" phx-click="results-action-change" phx-value-search_timezone="Etc/UTC">UTC</button>
        <span :if={show_checkbox?(assigns)} class="tw-relative tw-align-text-bottom">
          {checkbox(f, :remember_timezone, class: "tw-align-middle")}
          {label(f, :remember_timezone, "Remember", class: "tw-text-xs tw-my-0 tw-leading-3")}
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

  defp build_timezones_select_form_options do
    Timex.timezones()
    |> Enum.map(&[offset: Timex.Timezone.get(&1).offset_utc, t: &1])
    |> Enum.sort_by(& &1[:offset])
    |> Enum.map(fn [offset: offset, t: t] ->
      hoursstring = DateTimeUtils.humanize_timezone_offset(offset)

      {String.to_atom("#{t} (#{hoursstring})"), t}
    end)
  end
end
