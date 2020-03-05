defmodule LogflareWeb.SearchView do
  use LogflareWeb, :view
  import LogflareWeb.Helpers.Flash
  import LogflareWeb.Helpers.Modals

  alias LogflareWeb.Source
  alias LogflareWeb.SearchView
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.JSON

  import PhoenixLiveReact, only: [live_react_component: 2]

  def format_bq_schema(nil), do: ""

  def format_bq_schema(bq_schema) do
    fields_and_types =
      bq_schema
      |> SchemaUtils.bq_schema_to_flat_typemap()
      |> Enum.map(fn {k, v} ->
        v =
          case SchemaTypes.to_schema_type(v) do
            {type, inner_type} -> "#{type}<#{inner_type}>"
            type -> type
          end

        {k, v}
      end)
      |> Enum.sort_by(fn {k, _v} -> k end)

    SearchView.render("bq_schema.html", fields_and_types: fields_and_types)
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
    |> JSON.encode!(pretty: true)
  end
end
