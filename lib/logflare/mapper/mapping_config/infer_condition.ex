defmodule Logflare.Mapper.MappingConfig.InferCondition do
  @moduledoc """
  A single condition within an `InferRule`.

  Evaluates a JSONPath (`:path`) against the input document using a `:predicate`.
  Some predicates require `:comparison_value` or `:comparison_values`.

  See `Logflare.Mapper.MappingConfig.FieldConfig` for the full list of predicates.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @primary_key false
  typed_embedded_schema do
    field(:path, :string)
    field(:predicate, :string)
    field(:comparison_value, :string)
    field(:comparison_values, {:array, :string})
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [:path, :predicate, :comparison_value, :comparison_values])
    |> validate_required([:path, :predicate])
  end
end
