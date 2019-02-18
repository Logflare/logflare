defmodule Logflare.Source do
  use Ecto.Schema
  import Ecto.Changeset

  schema "sources" do
    field(:name, :string)
    field(:token, Ecto.UUID)
    field(:public_token, :string)
    belongs_to(:user, Logflare.User)
    has_many(:rules, Logflare.Rule)

    timestamps()
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [:name, :token, :public_token])
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
    |> unique_constraint(:public_token)
  end
end
