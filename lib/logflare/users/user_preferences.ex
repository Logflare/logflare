defmodule Logflare.Users.UserPreferences do
  use TypedEctoSchema
  import Ecto.Changeset

  typed_embedded_schema do
    field :timezone, :string
    field :search_use_local_time, :boolean, default: true
  end

  def changeset(user_prefs, attrs \\ %{}) do
    user_prefs
    |> cast(attrs, [:timezone, :search_use_local_time])
    |> validate_required([:timezone])
  end
end
