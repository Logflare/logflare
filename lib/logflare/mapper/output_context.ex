defmodule Logflare.Mapper.OutputContext do
  @moduledoc """
  Builds runtime context required by configured mapper output formats.

  Output selection remains part of the compiled mapping configuration. Context
  supplies only per-document values that cannot be serialized into that
  configuration.
  """

  alias Logflare.LogEvent

  @opaque t() ::
            {:ch_row_binary | :ndjson, binary(), {binary(), binary(), binary(), integer() | nil}}

  @doc "Builds the per-row context required by ClickHouse RowBinary output."
  @spec ch_row_binary(LogEvent.t(), binary()) :: t()
  def ch_row_binary(
        %LogEvent{
          id: id,
          source_uuid: source_uuid,
          source_name: source_name,
          ingested_at: ingested_at
        },
        mapping_config_id
      )
      when is_binary(mapping_config_id) do
    ingested_at = if ingested_at, do: DateTime.to_unix(ingested_at, :microsecond)
    source_uuid = if is_atom(source_uuid), do: Atom.to_string(source_uuid), else: source_uuid

    {:ch_row_binary, mapping_config_id, {id, source_uuid, source_name || "", ingested_at}}
  end

  @doc """
  Builds the per-row context required by NDJSON output.

  `mapping_config_id` is emitted as the UUID string. `ingested_at` is emitted
  as Unix **nanoseconds** (or `null`), which is the wire contract for
  consumers parsing the JSON; note that the RowBinary context uses
  microseconds to match ClickHouse's `DateTime64(6)` column.
  """
  @spec ndjson(LogEvent.t(), String.t()) :: t()
  def ndjson(
        %LogEvent{
          id: id,
          source_uuid: source_uuid,
          source_name: source_name,
          ingested_at: ingested_at
        },
        mapping_config_id
      )
      when is_binary(mapping_config_id) do
    ingested_at = if ingested_at, do: DateTime.to_unix(ingested_at, :nanosecond)
    source_uuid = if is_atom(source_uuid), do: Atom.to_string(source_uuid), else: source_uuid

    {:ndjson, mapping_config_id, {id, source_uuid, source_name || "", ingested_at}}
  end
end
