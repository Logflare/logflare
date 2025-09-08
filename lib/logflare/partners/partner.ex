defmodule Logflare.Partners.Partner do
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.User

  typed_schema "partners" do
    field :name, :string
    field :token, Ecto.UUID, autogenerate: {Ecto.UUID, :generate, []}

    many_to_many :users, User, join_through: "partner_users"
  end

  def changeset(partner, params) do
    partner
    |> cast(params, [:name])
    |> validate_required([:name])
    |> unique_constraint([:name])
    |> unique_constraint([:token])
  end
end
