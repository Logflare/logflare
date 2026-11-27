defmodule Logflare.Mapper.MappingConfig.InferCondition do
  @moduledoc """
  A single condition within an `InferRule`.

  Evaluates a dot-notation path (`:path`) against the input map, or the resolved value of an
  earlier field (`:from_output`), using a `:predicate`. Some predicates require
  `:comparison_value` or `:comparison_values`. `:case_insensitive` and `:coercion` are
  optional comparison options.

  See `Logflare.Mapper.MappingConfig.FieldConfig` for the full list of predicates.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    field(:path, :string)
    field(:from_output, :string)
    field(:predicate, :string)
    field(:comparison_value, :string)
    field(:comparison_values, {:array, :string})
    field(:case_insensitive, :boolean, default: false)
    field(:coercion, :string)
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [
      :path,
      :from_output,
      :predicate,
      :comparison_value,
      :comparison_values,
      :case_insensitive,
      :coercion
    ])
    |> validate_required([:predicate])
    |> validate_source()
    |> validate_inclusion(:coercion, ~w(strict lenient))
  end

  defp validate_source(changeset) do
    case {get_field(changeset, :path), get_field(changeset, :from_output)} do
      {nil, nil} -> add_error(changeset, :path, "can't be blank")
      {_path, nil} -> changeset
      {nil, _from_output} -> changeset
      _both -> add_error(changeset, :from_output, "cannot be set together with path")
    end
  end
end
