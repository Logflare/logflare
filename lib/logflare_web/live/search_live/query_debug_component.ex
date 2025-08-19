defmodule LogflareWeb.Search.QueryDebugComponent do
  @moduledoc """
  LiveView Component to render components
  """
  use LogflareWeb, :live_component

  def update(assigns, socket) do
    {:ok, assign(socket, assigns)}
  end

  def render(assigns) do
    assigns =
      assign_new(assigns, :search_op, fn
        %{id: :modal_debug_error_link} -> assigns.search_op_error
        %{id: :modal_debug_log_events_link} -> assigns.search_op_log_events
        %{id: :modal_debug_log_aggregates_link} -> assigns.search_op_log_aggregates
      end)

    ~H"""
    <div phx-hook="BigQuerySqlQueryFormatter" id="search-query-debug">
      <%= if @search_op do %>
        <div class="search-query-debug">
          <div>
            <% stats = @search_op.stats %>
            <ul class="list-group">
              <h5 class="header-margin">BigQuery Query</h5>
              <p>
                Actual SQL query used when querying for results. Use it in the BigQuery console if you need to.
              </p>
              <li class="list-group-item">
                <pre><code class="sql" id="search-op-sql-string"><%= @search_op.sql_string %></code></pre>
              </li>
            </ul>
            <ul class="list-group list-group-horizontal">
              <li class="list-group-item flex-fill">
                Total rows: <span class="my-badge my-badge-info"><%= stats[:total_rows] %></span>
              </li>
              <li class="list-group-item flex-fill">
                Total bytes processed: <span class="my-badge my-badge-info"><%= stats[:total_bytes_processed] %></span>
              </li>
              <li class="list-group-item flex-fill">
                Total duration: <span class="my-badge my-badge-info"><%= stats[:total_duration] %>ms</span>
              </li>
            </ul>
          </div>
        </div>
        <div>
          <%= if @user.admin do %>
            <ul class="list-group">
              <h5 class="header-margin">BigQuery</h5>
              <p>Viewable by Logflare admin only.</p>
              <%= link("View BigQuery table",
                to:
                  Logflare.Google.BigQuery.Debug.gen_bq_ui_url(
                    @user,
                    Atom.to_string(@search_op.source.token)
                  ),
                class: "btn btn-primary",
                target: "_blank"
              ) %>
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
