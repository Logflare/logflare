defmodule Logflare.Mapper.MappingConfig.PickEntry do
  @moduledoc """
  A single entry in a `FieldConfig` json field's `:pick` list.

  Each entry defines an output `:key` and coalesce `:paths` to try. If any path
  resolves, the key is included in the output map; otherwise it's omitted (sparse).
  """

  use TypedEctoSchema

  import Ecto.Changeset

  @primary_key false
  typed_embedded_schema do
    field(:key, :string)
    field(:paths, {:array, :string})
  end

  @spec changeset(t() | Ecto.Changeset.t(), map()) :: Ecto.Changeset.t()
  def changeset(struct_or_changeset, attrs) do
    struct_or_changeset
    |> cast(attrs, [:key, :paths])
    |> validate_required([:key, :paths])
  end
end
