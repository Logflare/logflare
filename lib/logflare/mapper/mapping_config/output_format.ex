defmodule Logflare.Mapper.MappingConfig.OutputFormat do
  @moduledoc """
  Defines the output produced by a compiled mapping configuration.

  Mapping configurations without an output format continue to produce maps.
  ClickHouse RowBinary output also records the row type so the mapper can
  compile one schema-specific field layout.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    field(:format, Ecto.Enum, values: [:clickhouse_row_binary])
    field(:row_type, Ecto.Enum, values: [:log, :metric, :trace])
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [:format, :row_type])
    |> validate_required([:format, :row_type])
  end

  @spec clickhouse_row_binary(:log | :metric | :trace) :: t()
  def clickhouse_row_binary(row_type) when row_type in [:log, :metric, :trace] do
    %__MODULE__{format: :clickhouse_row_binary, row_type: row_type}
  end

  @spec to_nif_map(t()) :: map()
  def to_nif_map(%__MODULE__{format: format, row_type: row_type}) do
    %{"format" => Atom.to_string(format), "row_type" => Atom.to_string(row_type)}
  end
end
