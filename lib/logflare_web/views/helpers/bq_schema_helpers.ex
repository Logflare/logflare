defmodule LogflareWeb.Helpers.BqSchema do
  @moduledoc false
  alias LogflareWeb.SharedView
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.JSON
  alias Logflare.Utils

  @type field_and_type :: {String.t(), String.t()}
  @type timestamp_format_opts :: [format: String.t()]

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

  @timestamp_format "%a %b %d %Y %H:%M:%S"

  @spec format_timestamp(integer(), String.t() | nil, timestamp_format_opts()) :: String.t()
  def format_timestamp(timestamp, search_timezone \\ nil, opts \\ [])
      when is_integer(timestamp) do
    format = Keyword.get(opts, :format, @timestamp_format)

    timestamp
    |> Utils.to_microseconds()
    |> DateTime.from_unix!(:microsecond)
    |> convert_timezone(search_timezone)
    |> Timex.format!(format, :strftime)
  end

  defp convert_timezone(%DateTime{} = datetime, timezone) when is_binary(timezone) do
    with %DateTime{} = converted <- Timex.Timezone.convert(datetime, timezone) do
      converted
    else
      _ -> datetime
    end
  end

  defp convert_timezone(%DateTime{} = datetime, _timezone), do: datetime

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
