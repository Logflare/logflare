defmodule Logflare.Mapper.MappingConfig.OutputFormat do
  @moduledoc """
  Defines the output produced by a compiled mapping configuration.

  Mapping configurations without an output format continue to produce maps.
  Serialized formats (ClickHouse RowBinary, NDJSON) also record the row type
  so the mapper can compile one schema-specific field layout and apply the
  derived-field rules for that type. Timestamp units are a property of the
  `datetime64` fields themselves (see `FieldConfig.datetime64/2`).

  NDJSON output takes two options for consumers that cannot store the default
  layout (e.g. BigQuery):

    * `column_names: :underscore` replaces `.` with `_` in column names. A name
      produced by more than one field is a compile error.
    * `nested_arrays: :json_strings` writes each element of an `array_flat_map`
      field as a JSON object string instead of a nested object.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @derive Jason.Encoder

  @ndjson_defaults [column_names: :dotted, nested_arrays: :native]

  @type ndjson_opts :: [
          column_names: :dotted | :underscore,
          nested_arrays: :native | :json_strings
        ]

  @primary_key false
  typed_embedded_schema do
    field(:format, Ecto.Enum, values: [:ch_row_binary, :ndjson])
    field(:row_type, Ecto.Enum, values: [:log, :metric, :trace])
    field(:column_names, Ecto.Enum, values: [:dotted, :underscore], default: :dotted)
    field(:nested_arrays, Ecto.Enum, values: [:native, :json_strings], default: :native)
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [:format, :row_type, :column_names, :nested_arrays])
    |> validate_required([:format, :row_type])
  end

  @spec ch_row_binary(:log | :metric | :trace) :: t()
  def ch_row_binary(row_type) when row_type in [:log, :metric, :trace] do
    %__MODULE__{format: :ch_row_binary, row_type: row_type}
  end

  @doc """
  NDJSON output: one JSON object per document, newline terminated, with the
  envelope fields followed by the mapped fields in config order. See the module
  docs for `opts`.
  """
  @spec ndjson(:log | :metric | :trace, ndjson_opts()) :: t()
  def ndjson(row_type, opts \\ []) when row_type in [:log, :metric, :trace] do
    struct!(__MODULE__, [format: :ndjson, row_type: row_type] ++ normalize_ndjson_opts(opts))
  end

  @doc """
  Returns NDJSON `opts` with defaults filled in, in a fixed order, so equivalent
  options compare equal. Raises `ArgumentError` on an unknown option.
  """
  @spec normalize_ndjson_opts(keyword()) :: ndjson_opts()
  def normalize_ndjson_opts(opts) do
    opts |> Keyword.validate!(@ndjson_defaults) |> Enum.sort()
  end

  @doc """
  Returns the NDJSON options used when none are given.
  """
  @spec ndjson_defaults() :: ndjson_opts()
  def ndjson_defaults, do: normalize_ndjson_opts([])

  @spec to_nif_map(t()) :: map()
  def to_nif_map(%__MODULE__{format: :ndjson} = output) do
    output
    |> base_nif_map()
    |> Map.merge(%{
      "column_names" => Atom.to_string(output.column_names),
      "nested_arrays" => Atom.to_string(output.nested_arrays)
    })
  end

  def to_nif_map(%__MODULE__{} = output), do: base_nif_map(output)

  @spec base_nif_map(t()) :: map()
  defp base_nif_map(%__MODULE__{format: format, row_type: row_type}) do
    %{"format" => Atom.to_string(format), "row_type" => Atom.to_string(row_type)}
  end
end
