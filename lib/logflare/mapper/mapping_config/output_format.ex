defmodule Logflare.Mapper.MappingConfig.OutputFormat do
  @moduledoc """
  Defines the output produced by a compiled mapping configuration.

  Mapping configurations without an output format continue to produce maps.
  Serialized formats (ClickHouse RowBinary, NDJSON) also record the row type
  so the mapper can compile one schema-specific field layout and apply the
  derived-field rules for that type.

  `timestamp_precision` declares the unit of every timestamp the format
  emits (9 = nanoseconds for ClickHouse `DateTime64(9)` columns, 6 =
  microseconds for Iceberg `timestamptz`); at compile time it overrides the
  precision of all `datetime64`/`array_datetime64` fields, see
  `Logflare.Mapper.MappingConfig.apply_timestamp_precision/1`.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    field(:format, Ecto.Enum, values: [:ch_row_binary, :ndjson])
    field(:row_type, Ecto.Enum, values: [:log, :metric, :trace])
    field(:timestamp_precision, :integer)
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [:format, :row_type, :timestamp_precision])
    |> validate_required([:format, :row_type])
    |> validate_number(:timestamp_precision,
      greater_than_or_equal_to: 0,
      less_than_or_equal_to: 9
    )
  end

  @spec ch_row_binary(:log | :metric | :trace) :: t()
  def ch_row_binary(row_type) when row_type in [:log, :metric, :trace] do
    %__MODULE__{format: :ch_row_binary, row_type: row_type, timestamp_precision: 9}
  end

  @doc """
  NDJSON output: one JSON object per document, newline terminated, with the
  envelope fields followed by the mapped fields in config order.
  """
  @spec ndjson(:log | :metric | :trace) :: t()
  def ndjson(row_type) when row_type in [:log, :metric, :trace] do
    %__MODULE__{format: :ndjson, row_type: row_type, timestamp_precision: 6}
  end

  @spec to_nif_map(t()) :: map()
  def to_nif_map(%__MODULE__{format: format, row_type: row_type}) do
    %{"format" => Atom.to_string(format), "row_type" => Atom.to_string(row_type)}
  end
end
