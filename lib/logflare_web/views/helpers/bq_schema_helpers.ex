defmodule LogflareWeb.Helpers.BqSchema do
  @moduledoc false
  alias LogflareWeb.SharedView
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.JSON

  def format_bq_schema(nil), do: ""

  def format_bq_schema(bq_schema) do
    SharedView.render("bq_schema.html",
      fields_and_types: format_bq_schema(bq_schema, type: :text)
    )
  end

  def format_bq_schema(bq_schema, type: :text) do
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
  end

  @fmt_string "%a %b %d %Y %H:%M:%S"
  def format_timestamp(timestamp) do
    timestamp
    |> Timex.from_unix(:microsecond)
    |> Timex.format!(@fmt_string, :strftime)
  end

  def format_timestamp(timestamp, search_timezone) do
    dt = Timex.from_unix(timestamp, :microsecond)

    case Timex.Timezone.convert(dt, search_timezone) do
      {:error, _} -> Timex.format!(dt, @fmt_string, :strftime)
      converted -> Timex.format!(converted, @fmt_string, :strftime)
    end
  end

  def encode_metadata(metadata) do
    metadata
    |> Iteraptor.map(
      fn
        {_, [val]} -> val
        {_, val} -> val
      end,
      yield: :all
    )
    |> JSON.encode!(pretty: true)
  end
end
