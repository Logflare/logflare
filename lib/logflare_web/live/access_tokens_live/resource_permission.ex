defmodule LogflareWeb.AccessTokensLive.ResourcePermission do
  @moduledoc "Validates ingest or query resource permissions in the access token form."

  use TypedEctoSchema

  import Ecto.Changeset, only: [add_error: 3, cast: 4, validate_required: 2]

  @primary_key false
  typed_embedded_schema do
    field(:enabled, :boolean, default: false)
    field(:mode, Ecto.Enum, values: [:all, :selected], default: :all)
    field(:selected_ids, {:array, :integer}, default: [])
  end

  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(permission, attrs) do
    attrs = Map.replace_lazy(attrs, "selected_ids", &reject_empty_values/1)

    permission
    |> cast(attrs, [:enabled, :mode, :selected_ids], empty_values: [])
    |> validate_required([:enabled, :mode])
    |> validate_submitted_fields(attrs)
  end

  # validation so field omission won't imply all resources
  defp validate_submitted_fields(changeset, attrs) do
    changeset =
      if Map.has_key?(attrs, "enabled"),
        do: changeset,
        else: add_error(changeset, :enabled, "can't be blank")

    if Map.get(attrs, "enabled") in [true, "true"] and not Map.has_key?(attrs, "mode"),
      do: add_error(changeset, :mode, "can't be blank"),
      else: changeset
  end

  defp reject_empty_values(values) do
    values
    |> List.wrap()
    |> Enum.reject(&(&1 == ""))
  end
end
