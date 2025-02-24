defmodule Logflare.Users.PartnerDetails do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset

  typed_embedded_schema do
    field :upgraded, :boolean
  end

  def changeset(params, attrs \\ %{}) do
    params
    |> cast(attrs, [:upgraded])
    |> validate_required([:upgraded])
  end
end
