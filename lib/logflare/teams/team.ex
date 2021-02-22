defmodule Logflare.Teams.Team do
  use TypedEctoSchema
  import Ecto.Changeset

  typed_schema "teams" do
    field :name, :string
    belongs_to :user, Logflare.User
    has_many :team_users, Logflare.TeamUsers.TeamUser

    timestamps()
  end

  use Logflare.Changefeeds.ChangefeedSchema

  @doc false
  def changeset(team, attrs) do
    team
    |> cast(attrs, [:name])
    |> validate_required([:name])
  end
end
