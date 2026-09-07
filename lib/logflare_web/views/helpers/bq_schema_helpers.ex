defmodule LogflareWeb.Helpers.BqSchema do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.JSON
  alias Logflare.Utils
  alias LogflareWeb.SharedView

  @type field_and_type :: {String.t(), String.t()}
  @type format_schema_opts :: [type: :markdown | :text]
  @type timestamp_format_opts :: [format: String.t()]

  @spec format_schema(map()) :: Phoenix.HTML.safe()
  def format_schema(schema_flatmap) when is_map(schema_flatmap) do
    SharedView.render("bq_schema.html",
      fields_and_types: format_schema(schema_flatmap, type: :text),
      markdown_schema: format_schema(schema_flatmap, type: :markdown)
    )
  end

  @spec format_bq_schema(TS.t(), format_schema_opts()) :: [field_and_type()] | String.t()
  def format_bq_schema(%TS{} = bq_schema, type: type) do
    bq_schema
    |> SchemaUtils.bq_schema_to_flat_typemap()
    |> format_schema(type: type)
  end

  @spec format_schema(map(), format_schema_opts()) :: [field_and_type()] | String.t()
  def format_schema(schema_flatmap, type: :text) when is_map(schema_flatmap) do
    schema_flatmap
    |> Enum.map(fn {field, type} -> {field, format_type(type)} end)
    |> Enum.sort_by(fn {field, _type} -> field end)
  end

  def format_schema(schema_flatmap, type: :markdown) when is_map(schema_flatmap) do
    [
      "# Logflare source schema",
      "",
      "Use this schema when writing Logflare LQL (https://docs.logflare.app/concepts/lql/)",
      ""
      | schema_flatmap
        |> format_schema(type: :text)
        |> Enum.map(fn {field, type} -> "- `#{field}` #{type}#{markdown_note(field)}" end)
    ]
    |> Enum.join("\n")
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

  defp format_type(type) do
    case SchemaTypes.to_schema_type(type) do
      {type, inner_type} -> "#{type}<#{inner_type}>"
      type -> type
    end
  end

  defp markdown_note("event_message"), do: " Human-readable event message."

  defp markdown_note("id"), do: " Event UUID."

  defp markdown_note("timestamp"), do: " Ingest timestamp."

  defp markdown_note(_field), do: ""

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
