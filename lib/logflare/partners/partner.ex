defmodule Logflare.Partners.Partner do
  use Ecto.Schema
  import Ecto.Changeset
  alias Logflare.User

  schema "partners" do
    field :name, :string
    field :token, Ecto.UUID, autogenerate: {Ecto.UUID, :generate, []}
    field :auth_token, Logflare.Encryption.Binary, autogenerate: {Ecto.UUID, :generate, []}

    many_to_many :users, User, join_through: "partner_users"
  end

  def changeset(partner, params) do
    partner
    |> cast(params, [:name])
    |> validate_required([:name])
    |> unique_constraint([:name])
    |> unique_constraint([:token])
  end

  def associate_user_changeset(partner, %User{} = user) do
    partner
    |> change()
    |> put_assoc(:users, [user])
  end
end
