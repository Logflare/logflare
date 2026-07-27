defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeEncoder do
  @moduledoc false

  import Logflare.Utils.Guards, only: [is_event_type: 1]

  alias Logflare.LogEvent
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper.Native

  @spec map_and_encode_row(
          LogEvent.t(),
          reference(),
          TypeDetection.event_type(),
          binary()
        ) :: binary()
  def map_and_encode_row(%LogEvent{} = event, compiled, event_type, mapping_config_id)
      when is_event_type(event_type) and is_binary(mapping_config_id) do
    event
    |> encode_document()
    |> Native.map_and_encode_clickhouse(compiled, event_type, mapping_config_id)
    |> unwrap_result()
  end

  defp encode_document(%LogEvent{
         body: body,
         id: id,
         source_uuid: source_uuid,
         source_name: source_name,
         ingested_at: ingested_at
       })
       when is_map(body) do
    ingested_at = if ingested_at, do: DateTime.to_unix(ingested_at, :microsecond)
    {body, {id, Atom.to_string(source_uuid), source_name || "", ingested_at}}
  end

  defp unwrap_result({:ok, row}), do: row

  defp unwrap_result({:error, reason}) do
    raise ArgumentError, "failed to encode ClickHouse row: #{reason}"
  end
end
