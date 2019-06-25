defmodule LogflareWeb.Source.SearchLV do
  @moduledoc false
  alias Logflare.Google.BigQuery.{GenUtils, Query}
  alias Logflare.{Source, Logs, LogEvent}
  use Phoenix.LiveView

  def render(assigns) do
    ~L"""
    <form phx-submit="search">
      <div class="form-group">
        <input class="form-control" type="text" name="q" value="<%= @query %>" placeholder="Enter regex to search for matching log messages..."
               <%= if @loading, do: "readonly" %>/>
      </div>
        <div>
        <button class="btn btn-primary form-button" type="submit"> Search </button>
        <%= if @loading do %>
         <div class="spinner-border text-info" role="status">
          <span class="sr-only">Loading...</span>
        </div>
      <% end %>
      </div>
    </form>
    <%= if @log_events do %>
    <ul id="logs-list" class="list-unstyled console-text-list">
      <%= @log_events |> Enum.with_index |> Enum.map(fn {log, inx} -> %>
        <li>
          <mark class="log-datestamp" data-timestamp="<%= log.body.timestamp %>"><%= Timex.from_unix(log.body.timestamp, :microsecond) |> Timex.to_naive_datetime() %></mark>
          <%= log.body.message %>
          <%= if map_size(log.body.metadata) > 0 do %>
          <a class="metadata-link" data-toggle="collapse" href="#metadata-<%= inx %>"aria-expanded="false">
            metadata
          </a>
          <div class="collapse metadata" id="metadata-<%= inx %>">
            <pre class="pre-metadata"><code><%= Jason.encode!(log.body.metadata, pretty: true) %></code></pre>
          </div>
          <% end %>
        </li>
      <% end) %>
    </ul>
    <% end %>
    """
  end

  def mount(%{source: source} = session, socket) do
    {:ok, assign(socket, query: nil, loading: false, log_events: [], source: source)}
  end

  def handle_event("search", %{"q" => query}, socket) when byte_size(query) <= 100 do
    send(self(), {:search, query})
    {:noreply, assign(socket, query: query, result: "Searching...", loading: true)}
  end

  def handle_info({:search, query}, socket) do
    %Source{} = source = socket.assigns.source
    {:ok, %{result: log_events}} = Logs.Search.utc_today(%{regex: query, source: source})
    log_events = if log_events do
      Enum.map(log_events, &LogEvent.make(&1, %{source: source}))
    end

    {:noreply, assign(socket, loading: false, log_events: log_events)}
  end
end
