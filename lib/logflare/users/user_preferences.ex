defmodule Logflare.Users.UserPreferences do
  use TypedEctoSchema
  import Ecto.Changeset

  typed_embedded_schema do
    field :timezone, :string
  end

  def changeset(user_prefs, attrs \\ %{}) do
    user_prefs
    |> cast(attrs, [:timezone])
    |> validate_required([:timezone])
  end
end
