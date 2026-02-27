defmodule Logflare.Mapper.MappingConfig.FieldConfig do
  @moduledoc """
  Defines a single field in a `Logflare.Mapper.MappingConfig`.

  Use the constructor functions (`string/2`, `json/2`, `enum8/2`, `array_string/2`,
  etc.) to build field configs. Each constructor accepts the field name and a keyword
  list of options.

  ## Common Options (all types)

    * `:path` — single JSONPath source (e.g. `"$.trace_id"`)
    * `:paths` — coalesce paths; first non-nil value wins (e.g. `["$.trace_id", "$.traceId"]`).
      For string fields, empty strings are also skipped during coalesce.
    * `:from_output` — read from an already-resolved field in the output map instead of the
      input document (e.g. `from_output: "severity_text"`). Fields are resolved in order, so
      the source field must be defined earlier in the config.
    * `:default` — fallback value when no path resolves
    * `:value_map` — `%{String.t() => integer()}` lookup applied to the resolved value.
      Useful for derived fields (e.g. mapping `"ERROR"` to `17` for severity numbers).

  ## Type-Specific Options

  ### `string/2`

    * `:transform` — `"upcase"` or `"downcase"`, applied after resolution
    * `:allowed_values` — list of permitted string values. After transform is applied,
      if the value is not in this list it is replaced with the field's default. Useful for
      `LowCardinality(String)` columns where arbitrary strings would pollute the index.

  ### `datetime64/2`

    * `:precision` — target precision 0-9 (default `9` for nanoseconds). Integer inputs are
      auto-detected by digit count: 1-10 digits = seconds, 11-13 = ms, 14-16 = us, 17+ = ns.
      ISO8601/RFC3339 strings are parsed via chrono.

  ### `enum8/2`

    * `:values` — `%{String.t() => integer()}` mapping enum labels to their integer values
      (e.g. `%{"gauge" => 1, "sum" => 2, "histogram" => 3}`). Lookup is case-insensitive.
    * `:infer` — list of `InferRule` structs for structural inference when no explicit value
      is found at the configured paths. Rules are evaluated in order; first match wins.

  ### `json/2`

    * `:exclude_keys` — top-level keys to remove from the output map
    * `:elevate_keys` — keys whose children are merged into the parent map (the key itself
      is removed). Existing top-level keys win over elevated children.
    * `:pick` — list of `{key, paths}` tuples for sparse map assembly. Each entry tries its
      coalesce paths; resolved entries are included in the output, unresolved are omitted.
      If pick produces a non-empty map, it becomes the field value. If empty, falls back
      to `:path`/`:paths`.

  ### `flat_map/2`

  Like `json/2` but flattens nested maps to dot-notation keys with values
  coerced according to `:value_type`. Designed for ClickHouse `Map(String, V)`
  columns where `V` depends on the value type.

    * `:value_type` — target value type for the flat map (default `"string"`).
      Currently only `"string"` is supported. Future value types (`"integer"`,
      `"float"`) will target `Map(String, Int64)`, `Map(String, Float64)`, etc.
    * Nested maps: `%{"a" => %{"b" => 1}}` → `%{"a.b" => "1"}`
    * Lists: JSON-encoded as strings (e.g. `[1, 2]` → `"[1,2]"`)
    * Scalars: coerced to string (`42` → `"42"`, `true` → `"true"`)
    * nil values: omitted from the output map
    * Accepts the same options as `json/2`: `:exclude_keys`, `:elevate_keys`, `:pick`

  ## Array Types

  Seven array constructors extract and coerce lists of values. All default to `[]`
  when the path resolves to nil or a non-list value.

  Wildcard paths work naturally with array types — `$.items[*].name` resolves to
  a flat list of the `name` field from each element, which the array type then
  coerces per-element.

  ### Common array option

    * `:filter_nil` — when `true`, nil elements are removed from the list before
      coercion. When `false` (default), nil elements are coerced to the inner
      type's zero value (`""`, `0`, `0.0`, `0` epoch, or `%{}`). Keeping the
      default preserves array length, which is important for OTEL parallel arrays
      (e.g. events, links, exemplars) that must stay aligned.

  ### `array_string/2`

  Extracts a list of strings. Non-string elements are coerced to strings.
  Covers both `Array(String)` and `Array(LowCardinality(String))` ClickHouse types.

  ### `array_uint64/2`

  Extracts a list of unsigned 64-bit integers. Negative values are clamped to 0,
  floats are truncated.

  ### `array_float64/2`

  Extracts a list of 64-bit floats. Integers and parseable strings are coerced.

  ### `array_datetime64/2`

    * `:precision` — target precision 0-9 (default `9` for nanoseconds). Each
      element is auto-detected and scaled, same as scalar `datetime64/2`.

  ### `array_json/2`

  Pass-through list — elements are not coerced. Nil elements default to `%{}`.

  ### `array_map/2`

  Extracts a list of maps. Non-map elements are always filtered out (regardless
  of `filter_nil`), protecting against ClickHouse insert failures at high throughput.

  ### `array_flat_map/2`

  Like `array_map/2` but each map element is flattened using the same logic as
  `flat_map/2`. Non-map elements are filtered out. Inherits `:value_type` from
  the scalar `flat_map` behavior (default `"string"`). Designed for ClickHouse
  `Array(Map(String, V))` columns.

  ## Inference Rules (`InferRule` / `InferCondition`)

  Used by `enum8/2` to infer a value from structural cues when explicit path lookup finds
  no match. Each rule has `:any` (OR) and `:all` (AND) condition lists plus a `:result`
  string that is looked up in the `:values` map.

  A rule matches when `(any is empty OR at least one any-condition matches) AND
  (all is empty OR every all-condition matches)`.

  ### Supported Predicates

  | Predicate        | Description                                  | Extra fields             |
  |------------------|----------------------------------------------|--------------------------|
  | `"exists"`       | Value is non-nil                             |                          |
  | `"not_exists"`   | Value is nil/missing                         |                          |
  | `"not_zero"`     | Numeric value != 0                           |                          |
  | `"is_zero"`      | Numeric value == 0                           |                          |
  | `"greater_than"` | Numeric value > threshold                    | `comparison_value`       |
  | `"less_than"`    | Numeric value < threshold                    | `comparison_value`       |
  | `"not_empty"`    | Non-empty string or list                     |                          |
  | `"is_empty"`     | Empty string or list                         |                          |
  | `"equals"`       | Exact value match                            | `comparison_value`       |
  | `"not_equals"`   | Value doesn't match                          | `comparison_value`       |
  | `"in"`           | Value is one of a set                        | `comparison_values`      |
  | `"is_string"`    | Value is a binary/string                     |                          |
  | `"is_number"`    | Value is integer or float                    |                          |
  | `"is_list"`      | Value is a list                              |                          |
  | `"is_map"`       | Value is a map                               |                          |
  """

  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Mapper.MappingConfig.InferRule
  alias Logflare.Mapper.MappingConfig.PickEntry

  @valid_types ~w(string uint8 uint32 uint64 int32 float64 bool enum8 datetime64 json flat_map array_string array_uint64 array_float64 array_datetime64 array_json array_map array_flat_map)
  @valid_transforms ~w(upcase downcase)
  @valid_value_types ~w(string)

  @type common_opts :: [
          path: String.t(),
          paths: [String.t()],
          from_output: String.t(),
          default: term()
        ]

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    field(:name, :string)
    field(:type, :string)
    field(:path, :string)
    field(:paths, {:array, :string})
    field(:default, :string)
    field(:precision, :integer)
    field(:transform, :string)
    field(:allowed_values, {:array, :string})
    field(:from_output, :string)
    field(:value_map, :map)
    field(:enum_values, :map)
    field(:exclude_keys, {:array, :string})
    field(:elevate_keys, {:array, :string})
    field(:filter_nil, :boolean, default: false)
    field(:value_type, :string)
    embeds_many(:pick, PickEntry)
    embeds_many(:infer, InferRule)
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(
      attrs,
      [
        :name,
        :type,
        :path,
        :paths,
        :default,
        :precision,
        :transform,
        :allowed_values,
        :from_output,
        :value_map,
        :enum_values,
        :exclude_keys,
        :elevate_keys,
        :filter_nil,
        :value_type
      ],
      empty_values: []
    )
    |> validate_required([:name, :type])
    |> validate_inclusion(:type, @valid_types)
    |> validate_inclusion(:transform, @valid_transforms)
    |> validate_inclusion(:value_type, @valid_value_types)
    |> cast_embed(:pick, with: &PickEntry.changeset/2)
    |> cast_embed(:infer, with: &InferRule.changeset/2)
  end

  @spec string(String.t(), keyword()) :: t()
  def string(name, opts \\ []) do
    build(name, "string", opts, [:transform, :allowed_values])
  end

  @spec uint8(String.t(), keyword()) :: t()
  def uint8(name, opts \\ []) do
    build(name, "uint8", opts)
  end

  @spec uint32(String.t(), keyword()) :: t()
  def uint32(name, opts \\ []) do
    build(name, "uint32", opts)
  end

  @spec uint64(String.t(), keyword()) :: t()
  def uint64(name, opts \\ []) do
    build(name, "uint64", opts)
  end

  @spec int32(String.t(), keyword()) :: t()
  def int32(name, opts \\ []) do
    build(name, "int32", opts)
  end

  @spec float64(String.t(), keyword()) :: t()
  def float64(name, opts \\ []) do
    build(name, "float64", opts)
  end

  @spec bool(String.t(), keyword()) :: t()
  def bool(name, opts \\ []) do
    build(name, "bool", opts)
  end

  @spec enum8(String.t(), keyword()) :: t()
  def enum8(name, opts \\ []) do
    base = build(name, "enum8", opts)

    base
    |> maybe_put(:enum_values, opts[:values])
    |> maybe_put_infer(opts[:infer])
  end

  @spec datetime64(String.t(), keyword()) :: t()
  def datetime64(name, opts \\ []) do
    base = build(name, "datetime64", opts)
    %{base | precision: opts[:precision] || 9}
  end

  @spec json(String.t(), keyword()) :: t()
  def json(name, opts \\ []) do
    base = build(name, "json", opts, [:exclude_keys, :elevate_keys])

    base
    |> maybe_put_pick(opts[:pick])
  end

  @spec array_string(String.t(), keyword()) :: t()
  def array_string(name, opts \\ []) do
    build(name, "array_string", opts, [:filter_nil])
  end

  @spec array_uint64(String.t(), keyword()) :: t()
  def array_uint64(name, opts \\ []) do
    build(name, "array_uint64", opts, [:filter_nil])
  end

  @spec array_float64(String.t(), keyword()) :: t()
  def array_float64(name, opts \\ []) do
    build(name, "array_float64", opts, [:filter_nil])
  end

  @spec array_datetime64(String.t(), keyword()) :: t()
  def array_datetime64(name, opts \\ []) do
    base = build(name, "array_datetime64", opts, [:filter_nil])
    %{base | precision: opts[:precision] || 9}
  end

  @spec array_json(String.t(), keyword()) :: t()
  def array_json(name, opts \\ []) do
    build(name, "array_json", opts, [:filter_nil])
  end

  @spec array_map(String.t(), keyword()) :: t()
  def array_map(name, opts \\ []) do
    build(name, "array_map", opts, [:filter_nil])
  end

  @spec flat_map(String.t(), keyword()) :: t()
  def flat_map(name, opts \\ []) do
    opts = Keyword.put_new(opts, :value_type, "string")
    base = build(name, "flat_map", opts, [:exclude_keys, :elevate_keys, :value_type])
    maybe_put_pick(base, opts[:pick])
  end

  @spec array_flat_map(String.t(), keyword()) :: t()
  def array_flat_map(name, opts \\ []) do
    opts = Keyword.put_new(opts, :value_type, "string")
    build(name, "array_flat_map", opts, [:filter_nil, :value_type])
  end

  defp build(name, type, opts, extra_keys \\ []) do
    base = %__MODULE__{
      name: name,
      type: type,
      path: opts[:path],
      paths: opts[:paths],
      from_output: opts[:from_output],
      default: encode_default(opts[:default]),
      value_map: opts[:value_map]
    }

    Enum.reduce(extra_keys, base, fn key, acc ->
      maybe_put(acc, key, opts[key])
    end)
  end

  defp encode_default(nil), do: nil
  defp encode_default(val) when is_binary(val), do: val
  defp encode_default(val) when is_integer(val), do: Integer.to_string(val)
  defp encode_default(val) when is_float(val), do: Float.to_string(val)
  defp encode_default(true), do: "true"
  defp encode_default(false), do: "false"
  defp encode_default(val) when is_map(val), do: "{}"
  defp encode_default(val) when is_list(val), do: "[]"

  defp maybe_put(struct, _key, nil), do: struct
  defp maybe_put(struct, key, value), do: Map.put(struct, key, value)

  defp maybe_put_pick(struct, nil), do: struct

  defp maybe_put_pick(struct, entries) when is_list(entries) do
    pick =
      Enum.map(entries, fn
        {key, paths} -> %PickEntry{key: key, paths: paths}
        %PickEntry{} = entry -> entry
      end)

    %{struct | pick: pick}
  end

  defp maybe_put_infer(struct, nil), do: struct

  defp maybe_put_infer(struct, rules) when is_list(rules) do
    %{struct | infer: rules}
  end
end
