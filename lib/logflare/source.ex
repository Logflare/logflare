defmodule Logflare.Source do
  use Ecto.Schema
  import Ecto.Changeset

  schema "sources" do
    field :name, :string
    field :token, Ecto.UUID
    belongs_to :user, Logflare.User

    timestamps()
  end

  @doc false
  def changeset(source, attrs) do
    source
    |> cast(attrs, [:name, :token])
    |> validate_required([:name, :token])
    |> unique_constraint(:name)
  end
end
