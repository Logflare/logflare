defmodule LogflareWeb.AccessTokensLive.ResourcePermission do
  @moduledoc "Validates ingest or query resource permissions in the access token form."

  use TypedEctoSchema

  import Ecto.Changeset, only: [cast: 4, validate_required: 2]

  @primary_key false
  typed_embedded_schema do
    field(:enabled, :boolean, default: false)
    field(:mode, Ecto.Enum, values: [:all, :selected], default: :selected)
    field(:selected_ids, {:array, :integer}, default: [])
  end

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(permission, attrs) do
    permission
    |> cast(attrs, [:enabled, :mode, :selected_ids],
      trim_values: &blank_integer_to_empty/2,
      empty_values: [:blank]
    )
    |> validate_required([:enabled, :mode])
  end

  defp blank_integer_to_empty(:integer, ""), do: :blank
  defp blank_integer_to_empty(_type, value), do: value
end
