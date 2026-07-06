defmodule LogflareWeb.Helpers.BqSchema do
  @moduledoc false
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias Logflare.BigQuery.SchemaTypes
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.JSON
  alias Logflare.Utils
  alias LogflareWeb.SharedView

  @type field_and_type :: {String.t(), String.t()}
  @type format_bq_schema_opts :: [type: :markdown | :text]
  @type timestamp_format_opts :: [format: String.t()]

  @spec format_bq_schema(nil) :: String.t()
  def format_bq_schema(nil), do: ""

  @spec format_bq_schema(TS.t()) :: Phoenix.HTML.safe()
  def format_bq_schema(bq_schema) do
    SharedView.render("bq_schema.html",
      fields_and_types: format_bq_schema(bq_schema, type: :text),
      markdown_schema: format_bq_schema(bq_schema, type: :markdown)
    )
  end

  @spec format_bq_schema(TS.t(), format_bq_schema_opts()) :: [field_and_type()] | String.t()
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

  def format_bq_schema(%TS{fields: fields}, type: :markdown) do
    [
      "# Logflare source schema",
      "",
      "Use this schema when writing Logflare LQL (https://docs.logflare.app/concepts/lql/)",
      ""
      | markdown_field_lines(fields || [])
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

  defp markdown_field_lines(fields, path \\ [], depth \\ 0)

  defp markdown_field_lines(fields, path, depth) when is_list(fields) do
    fields
    |> Enum.sort_by(& &1.name)
    |> Enum.flat_map(&markdown_field_lines(&1, path, depth))
  end

  defp markdown_field_lines(%TFS{name: name, fields: fields} = field, path, depth) do
    current_path = path ++ [name]

    [markdown_field_line(field, current_path, depth)] ++
      markdown_field_lines(fields || [], current_path, depth + 1)
  end

  defp markdown_field_line(%TFS{name: name, type: type, mode: mode}, path, depth) do
    "#{markdown_indent(depth)}- `#{name}` #{markdown_type(type, mode)}#{markdown_note(path)}"
  end

  defp markdown_type(type, "REPEATED"), do: "ARRAY<#{type}>"
  defp markdown_type(type, _mode), do: type

  defp markdown_note(["event_message"]), do: " Human-readable event message."

  defp markdown_note(["id"]), do: " Event UUID."

  defp markdown_note(["timestamp"]), do: " Ingest timestamp."

  defp markdown_note(_path), do: ""

  defp markdown_indent(depth), do: String.duplicate("  ", depth)

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
