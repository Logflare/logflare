defmodule LogflareWeb.Source.SearchLV.ModalLVC do
  use Phoenix.LiveComponent
  alias LogflareWeb.SearchView
  alias Logflare.Lql
  alias Logflare.Sources
  alias LogflareWeb.LqlView
  import LogflareWeb.LiveComponentUtils

  def render(assigns) do
    log_events = assigns[:log_events]
    source = assigns[:source]

    case assigns.active_modal do
      "searchHelpModal" ->
        SearchView.render("modal.html",
          id: "searchHelpModal",
          title: "Logflare Query Language",
          body: SearchView.render("lql_help.html")
        )

      "sourceSchemaModal" ->
        SearchView.render("modal.html",
          id: "sourceSchemaModal",
          title: "Source Schema",
          body: SearchView.format_bq_schema(assigns.source)
        )

      "metadataModal:" <> id ->
        log_event =
          Enum.find(log_events, &(&1.id === id)) ||
            Enum.find(log_events, &("#{&1.body.timestamp}" === id))

        fmt_metadata =
          log_event
          |> Map.get(:body)
          |> Map.get(:metadata)
          |> SourceView.encode_metadata()

        body =
          SearchView.render("metadata_modal_body.html",
            log_event: log_event,
            fmt_metadata: fmt_metadata
          )

        SearchView.search_view("modal.html",
          id: "metadataModal",
          title: "Metadata",
          body: body
        )

      modal
      when modal in ~w(queryDebugEventsModal queryDebugErrorModal queryDebugAggregatesModal) ->
        {first, rest} = String.split_at(modal, 1)
        hook = "Source" <> String.capitalize(first) <> rest

        ~L"""
        <div class="source-logs-search-modals" phx-hook="<%= hook %>">
          <%= SearchView.render "modal.html",
            id: modal,
            title: "Query Debugging",
            body: "No query or query is still in progress..." %>
        </div>
        """

      _ ->
        ~L"""
        <div class="source-logs-search-modals"> </div>
        """
    end
  end

  def update(assigns, socket) do
    socket = assign(socket, assigns)

    {:ok, socket}
  end

  def mount(socket) do
    {:ok, socket}
  end
end
