defmodule Logflare.Teams.Team do
  @moduledoc false
  import Ecto.Changeset
  use TypedEctoSchema

  @derive {Jason.Encoder, only: [:name, :user, :team_users, :token]}
  typed_schema "teams" do
    field :name, :string
    field :token, :string, autogenerate: {Ecto.UUID, :generate, []}
    belongs_to :user, Logflare.User
    has_many :team_users, Logflare.TeamUsers.TeamUser

    timestamps()
  end

  @doc false
  def changeset(team, attrs) do
    team
    |> cast(attrs, [:name])
    |> validate_required([:name])
    |> unique_constraint(:token)
  end
end
