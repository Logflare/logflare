defmodule LogflareWeb.Search.QueryDebugComponent do
  @moduledoc """
  LiveView Component to render components
  """
  use LogflareWeb, :live_component

  alias LogflareWeb.Utils
  alias LogflareWeb.QueryComponents
  alias Logflare.Teams

  def update(assigns, socket) do
    {:ok, assign(socket, assigns)}
  end

  def render(assigns) do
    assigns =
      assign_new(assigns, :team, fn
        %{team_user: team_user} ->
          Teams.get_team!(team_user.team_id)
      end)
      |> assign_new(:search_op, fn
        %{id: :modal_debug_error_link} -> assigns.search_op_error
        %{id: :modal_debug_log_events_link} -> assigns.search_op_log_events
        %{id: :modal_debug_log_aggregates_link} -> assigns.search_op_log_aggregates
      end)
      |> assign_new(:sql_query, fn
        %{search_op: search_op} when not is_nil(search_op) ->
          Utils.sql_params_to_sql(search_op.sql_string, search_op.sql_params)
          |> Utils.replace_table_with_source_name(search_op.source)

        _ ->
          nil
      end)

    ~H"""
    <div id="search-query-debug">
      <%= if @search_op do %>
        <div class="search-query-debug">
          <div>
            <% stats = @search_op.stats %>

            <div class="tw-flex tw-items-start tw-gap-4">
              <div class="tw-flex-1">
                <h5 class="header-margin">BigQuery Query</h5>
                <p>
                  Actual SQL query used when querying for results. Use it in the BigQuery console if you need to.
                </p>
              </div>
              <.team_link :if={not is_nil(@sql_query)} team={@team} href={~p"/query?#{%{q: @sql_query}}"} class="btn btn-primary tw-flex-shrink-0 tw-self-start tw-mt-4">
                Edit as query
              </.team_link>
            </div>
            <ul class="list-group">
              <li class="list-group-item">
                <QueryComponents.formatted_sql sql_string={@search_op.sql_string} params={@search_op.sql_params} />
              </li>
            </ul>
            <ul class="list-group list-group-horizontal">
              <li class="list-group-item flex-fill">
                Total rows: <span class="my-badge my-badge-info">{stats[:total_rows]}</span>
              </li>
              <li class="list-group-item flex-fill">
                Total bytes processed: <span class="my-badge my-badge-info">{stats[:total_bytes_processed]}</span>
              </li>
              <li class="list-group-item flex-fill">
                Total duration: <span class="my-badge my-badge-info">{stats[:total_duration]}ms</span>
              </li>
            </ul>
          </div>
        </div>
        <div>
          <%= if @user.admin do %>
            <ul class="list-group">
              <h5 class="header-margin">BigQuery</h5>
              <p>Viewable by Logflare admin only.</p>
              {link("View BigQuery table",
                to:
                  Logflare.Google.BigQuery.Debug.gen_bq_ui_url(
                    @user,
                    Atom.to_string(@search_op.source.token)
                  ),
                class: "btn btn-primary",
                target: "_blank"
              )}
            </ul>
            <ul class="list-group">
              <h5 class="header-margin">Ecto Query</h5>
              <p>Viewable by Logflare admin only.</p>
              <li class="list-group-item flex-fill">
                <div>
                  <pre>
              <code class="elixir" id="search-op-query">
                <%= inspect(@search_op.query, width: 60, pretty: true) %>
              </code>
              </pre>
                </div>
              </li>
            </ul>
            <ul class="list-group">
              <h5 class="header-margin">LQL Timestamp Filters</h5>
              <p>Viewable by Logflare admin only.</p>
              <li class="list-group-item flex-fill">
                <div>
                  <pre>
                <code class="elixir" id="search-op-timestamp-filter-rules">
                <%= inspect(@search_op.lql_ts_filters, width: 60, pretty: true) %>
              </code>
              </pre>
                </div>
              </li>
            </ul>
            <ul class="list-group">
              <h5 class="header-margin">Metadata and Message Filters</h5>
              <p>Viewable by Logflare admin only.</p>
              <li class="list-group-item flex-fill">
                <div>
                  <pre>
                <code class="elixir" id="search-op-metadata-message-filter-rules">
                <%= inspect(@search_op.lql_meta_and_msg_filters, width: 60, pretty: true) %>
              </code>
              </pre>
                </div>
              </li>
            </ul>
          <% end %>
        </div>
      <% end %>
    </div>
    """
  end
end
