defmodule LogflareWeb.SearchLive.SubheadComponents do
  @moduledoc """
  Subheader components for logs search page.
  """
  use LogflareWeb, :html
  use LogflareWeb, :routes

  use Phoenix.Component

  import LogflareWeb.ModalLiveHelpers, only: [modal_link: 1]
  import LogflareWeb.SearchLive.TimezoneComponent

  alias LogflareWeb.LqlHelpers

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
          <span id="search-uri-query" class="pointer-cursor" data-trigger="hover focus" data-delay="0" data-toggle="tooltip" data-placement="top" data-title="<span id=&quot;copy-tooltip&quot;>Copy uri</span>">
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
end
