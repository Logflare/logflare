defmodule LogflareWeb.SearchView do
  use LogflareWeb, :view
  import LogflareWeb.Helpers.Flash

  alias LogflareWeb.Source
  alias LogflareWeb.SearchView
  alias Logflare.Sources
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Lql

  import PhoenixLiveReact, only: [live_react_component: 2]

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
        |> Lql.Utils.bq_schema_to_flat_typemap()
        |> Enum.map(fn {k, v} -> {k, SchemaTypes.to_schema_type(v)} end)
        |> Enum.sort_by(fn {k, _v} -> k end)

      SearchView.render("bq_schema.html", fields_and_types: fields_and_types)
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

  def modal_link(modal_id, icon_classes, text) do
    ~E"""
    <a class="modal-link" href="#" phx-click="activate_modal" phx-value-modal_id="<%= modal_id %>"><span><i class="<%= icon_classes %>"></i></span> <span class="hide-on-mobile"><%= text %></span></a>
    """
  end
end
