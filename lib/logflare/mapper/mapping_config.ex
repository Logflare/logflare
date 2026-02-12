defmodule Logflare.Mapper.MappingConfig do
  @moduledoc """
  Defines a mapping configuration for `Logflare.Mapper`.

  A mapping config is a list of `FieldConfig` structs, each describing how to
  extract and coerce a single output field from an input document. Build configs
  using the `FieldConfig` constructor functions, then compile via `Mapper.compile!/1`.

  ## Example

      alias Logflare.Mapper.MappingConfig
      alias Logflare.Mapper.MappingConfig.FieldConfig, as: Field

      config = MappingConfig.new([
        Field.string("trace_id", paths: ["$.trace_id", "$.traceId"], default: ""),
        Field.string("severity_text", paths: ["$.level"], transform: "upcase",
          default: "INFO", allowed_values: ~w(INFO WARN ERROR)),
        Field.uint8("severity_number", from_output: "severity_text",
          value_map: %{"INFO" => 9, "ERROR" => 17}, default: 0),
        Field.datetime64("timestamp", path: "$.timestamp"),
        Field.json("attributes", path: "$",
          exclude_keys: ["id", "timestamp"], elevate_keys: ["metadata"])
      ])

      compiled = Logflare.Mapper.compile!(config)
      Logflare.Mapper.map(document, compiled)
  """

  use TypedEctoSchema

  import Ecto.Changeset
  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  alias __MODULE__.FieldConfig
  alias __MODULE__.InferCondition
  alias __MODULE__.InferRule
  alias __MODULE__.PickEntry

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    embeds_many(:fields, FieldConfig)
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [])
    |> cast_embed(:fields, with: &FieldConfig.changeset/2)
  end

  @spec new([FieldConfig.t()]) :: t()
  def new(fields) when is_list(fields) do
    %__MODULE__{fields: fields}
  end

  @spec to_json(t()) :: {:ok, String.t()} | {:error, Jason.EncodeError.t()}
  def to_json(%__MODULE__{} = config), do: Jason.encode(config)

  @spec from_json(String.t()) :: {:ok, t()} | {:error, Ecto.Changeset.t() | Jason.DecodeError.t()}
  def from_json(json) when is_binary(json) do
    with {:ok, map} <- Jason.decode(json) do
      %__MODULE__{}
      |> changeset(map)
      |> Ecto.Changeset.apply_action(:insert)
    end
  end

  @spec to_nif_map(t()) :: map()
  def to_nif_map(%__MODULE__{fields: fields}) do
    %{"fields" => Enum.map(fields, &field_to_nif_map/1)}
  end

  @spec field_to_nif_map(FieldConfig.t()) :: map()
  defp field_to_nif_map(%FieldConfig{} = f) do
    base = %{"name" => f.name, "type" => f.type}

    base
    |> maybe_add("path", f.path)
    |> maybe_add("paths", f.paths)
    |> maybe_add("default", encode_nif_default(f))
    |> maybe_add("precision", f.precision)
    |> maybe_add("transform", f.transform)
    |> maybe_add("allowed_values", f.allowed_values)
    |> maybe_add("from_output", f.from_output)
    |> maybe_add("value_map", f.value_map)
    |> maybe_add("enum_values", f.enum_values)
    |> maybe_add("exclude_keys", f.exclude_keys)
    |> maybe_add("elevate_keys", f.elevate_keys)
    |> maybe_add_pick(f.pick)
    |> maybe_add_infer(f.infer)
  end

  @spec encode_nif_default(FieldConfig.t()) :: term()
  defp encode_nif_default(%FieldConfig{default: nil}), do: nil

  defp encode_nif_default(%FieldConfig{default: val, type: type})
       when type in ["uint8", "uint32", "uint64", "int32", "float64", "enum8", "datetime64"] do
    parse_numeric_default(val, type)
  end

  defp encode_nif_default(%FieldConfig{default: "true", type: "bool"}), do: true
  defp encode_nif_default(%FieldConfig{default: "false", type: "bool"}), do: false
  defp encode_nif_default(%FieldConfig{default: "{}", type: "json"}), do: %{}
  defp encode_nif_default(%FieldConfig{default: "[]"}), do: []
  defp encode_nif_default(%FieldConfig{default: val}), do: val

  @spec parse_numeric_default(term(), String.t()) :: number()
  defp parse_numeric_default(s, type) when is_non_empty_binary(s) do
    case Float.parse(s) do
      {f, ""} when type == "float64" -> f
      {f, ""} -> trunc(f)
      _ -> 0
    end
  end

  defp parse_numeric_default(_val, _type), do: 0

  @spec maybe_add(map(), String.t(), term()) :: map()
  defp maybe_add(map, _key, nil), do: map
  defp maybe_add(map, _key, []), do: map
  defp maybe_add(map, key, value), do: Map.put(map, key, value)

  @spec maybe_add_pick(map(), [PickEntry.t()]) :: map()
  defp maybe_add_pick(map, []), do: map

  defp maybe_add_pick(map, entries) do
    pick =
      Enum.map(entries, fn %PickEntry{} = e ->
        %{"key" => e.key, "paths" => e.paths}
      end)

    Map.put(map, "pick", pick)
  end

  @spec maybe_add_infer(map(), [InferRule.t()]) :: map()
  defp maybe_add_infer(map, []), do: map

  defp maybe_add_infer(map, rules) do
    infer =
      Enum.map(rules, fn %InferRule{} = r ->
        base = %{"result" => r.result}

        base
        |> maybe_add_conditions("any", r.any)
        |> maybe_add_conditions("all", r.all)
      end)

    Map.put(map, "infer", infer)
  end

  @spec maybe_add_conditions(map(), String.t(), [InferCondition.t()] | nil) :: map()
  defp maybe_add_conditions(map, _key, nil), do: map
  defp maybe_add_conditions(map, _key, []), do: map

  defp maybe_add_conditions(map, key, conditions) do
    conds =
      Enum.map(conditions, fn %InferCondition{} = c ->
        base = %{"path" => c.path, "predicate" => c.predicate}

        base
        |> maybe_add("comparison_value", c.comparison_value)
        |> maybe_add("comparison_values", c.comparison_values)
      end)

    Map.put(map, key, conds)
  end
end
