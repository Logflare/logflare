defmodule Logflare.Mapper.OutputContext do
  @moduledoc """
  Builds runtime context required by configured mapper output formats.

  Output selection remains part of the compiled mapping configuration. Context
  supplies only per-document values that cannot be serialized into that
  configuration.
  """

  alias Logflare.LogEvent

  @opaque t() ::
            {:clickhouse_row_binary, binary(), {binary(), binary(), binary(), integer() | nil}}

  @doc "Builds the per-row context required by ClickHouse RowBinary output."
  @spec clickhouse_row_binary(LogEvent.t(), binary()) :: t()
  def clickhouse_row_binary(
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

    {:clickhouse_row_binary, mapping_config_id, {id, source_uuid, source_name || "", ingested_at}}
  end
end
