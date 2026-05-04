defmodule Logflare.Mapper.MappingConfig.InferRule do
  @moduledoc """
  An inference rule for `FieldConfig` enum8 fields.

  When explicit path lookup finds no value, rules are evaluated in order.
  A rule matches when `(any is empty OR at least one matches) AND
  (all is empty OR every one matches)`. The `:result` string is then
  looked up in the parent enum8's `:values` map.

  See `Logflare.Mapper.MappingConfig.FieldConfig` for the full list of predicates.
  """

  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Mapper.MappingConfig.InferCondition

  @derive Jason.Encoder

  @primary_key false
  typed_embedded_schema do
    field(:result, :string)
    embeds_many(:any, InferCondition)
    embeds_many(:all, InferCondition)
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [:result])
    |> validate_required([:result])
    |> cast_embed(:any, with: &InferCondition.changeset/2)
    |> cast_embed(:all, with: &InferCondition.changeset/2)
  end
end
