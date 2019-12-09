defmodule LogflareWeb.SourceView do
  use LogflareWeb, :view
  import LogflareWeb.Helpers.Flash
  alias Logflare.{Sources, Logs}
  alias Logflare.BigQuery.SchemaTypes
  import Phoenix.LiveView
  import PhoenixLiveReact, only: [live_react_component: 2]

  def render_modal("searchHelpModal", _source, _log_events) do
    render("logs_search_modal.html",
      id: "searchHelpModal",
      title: "Logflare Query Language",
      body: render("logs_search_help.html")
    )
  end

  def render_modal("sourceSchemaModal", source, _log_events) do
    render("logs_search_modal.html",
      id: "sourceSchemaModal",
      title: "Source Schema",
      body: format_bq_schema(source)
    )
  end

  def render_modal("metadataModal:" <> id, _source, log_events) do
    log_event =
      Enum.find(log_events, &(&1.id === id)) || Enum.find(log_events, &(&1.timestamp === id))

    fmt_metadata =
      log_event
      |> Map.get(:body)
      |> Map.get(:metadata)
      |> encode_metadata

    body =
      render("logs_search_metadata_modal_body.html",
        log_event: log_event,
        fmt_metadata: fmt_metadata
      )

    render("logs_search_modal.html",
      id: "metadataModal",
      title: "Metadata",
      body: body
    )
  end

  def render_modal(id, _source, _log_events)
      when id in ~w(queryDebugEventsModal queryDebugErrorModal queryDebugAggregatesModal) do
    {first, rest} = String.split_at(id, 1)
    hook = "Source" <> String.capitalize(first) <> rest

    ~E"""
    <div class="source-logs-search-modals" phx-hook="<%= hook %>">
      <%= render "logs_search_modal.html",
        id: id,
        title: "Query Debugging",
        body: "No query or query is still in progress..." %>
    </div>
    """
  end

  def render_modal(_, _source) do
    ~E"""
    <div class="source-logs-search-modals"> </div>
    """
  end

  def format_sql({sql, params}) do
    Enum.reduce(params, sql, fn param, sql ->
      type = param.parameterType.type
      value = param.parameterValue.value

      case type do
        "STRING" ->
          String.replace(sql, "?", "'#{value}'", global: false)

        num when num in ~w(INTEGER FLOAT) ->
          String.replace(sql, "?", inspect(value), global: false)

        _ ->
          String.replace(sql, "?", inspect(value), global: false)
      end
    end)
  end

  def format_bq_schema(source) do
    bq_schema = Sources.Cache.get_bq_schema(source)

    if bq_schema do
      fields_and_types =
        bq_schema
        |> Logs.Validators.BigQuerySchemaChange.to_typemap()
        |> Iteraptor.to_flatmap()
        |> Enum.reject(fn {_, v} -> v == :map end)
        |> Enum.map(fn {k, v} -> {String.replace(k, ".fields.", "."), v} end)
        |> Enum.map(fn {k, v} -> {String.trim_trailing(k, ".t"), v} end)
        |> Enum.map(fn {k, v} -> {k, SchemaTypes.to_schema_type(v)} end)
        |> Enum.sort_by(fn {k, _v} -> k end)

      ~E"""
      <div class="table-responsive" phx-hook="SourceSchemaModalTable">
        <table class="table table-dark show-source-schema">
          <thead>
            <td>Field path</td>
            <td>BigQuery SQL type</td>
          </thead>
          <tbody>
            <%= for {field, type} <- fields_and_types do %>
            <tr>
              <td class="metadata-field">
              <a href="#">
              <span class="copy-metadata-field"
              data-clipboard-text="<%= field %>">
              <i style="color:green;" class="fas fa-copy"></i>
              </span></a>
              <span class="metadata-field-value"> <%= field %> </span> </td>
              <td><%= type %></td>
            </tr>
            <% end %>
          </tbody>
        </table>
      </div>
      """
    else
      ""
    end
  end

  def format_timestamp(timestamp) do
    timestamp
    |> Timex.from_unix(:microsecond)
    |> Timex.format!("%a %b %d %Y %I:%M:%S%p", :strftime)
  end

  def format_timestamp(timestamp, user_local_timezone) do
    timestamp
    |> Timex.from_unix(:microsecond)
    |> Timex.Timezone.convert(user_local_timezone)
    |> Timex.format!("%a %b %d %Y %I:%M:%S%p", :strftime)
  end

  def encode_metadata(metadata) do
    metadata
    |> Iteraptor.map(
      fn
        {_, [val]} ->
          val

        {_, val} ->
          val
      end,
      yield: :all
    )
    |> Jason.encode!(pretty: true)
  end
end
