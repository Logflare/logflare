defmodule Logflare.TeamUsers.TeamRole do
  @moduledoc false
  import Ecto.Changeset
  use TypedEctoSchema

  alias Logflare.TeamUsers.TeamUser

  @primary_key false
  @roles ~w(user admin)a

  typed_schema "team_roles" do
    field :role, Ecto.Enum, values: @roles

    belongs_to :team_user, TeamUser

    timestamps()
  end

  @doc false
  @spec changeset(t(), map()) :: Ecto.Changeset.t()
  def changeset(team_role, attrs) do
    team_role
    |> cast(attrs, [:role, :team_user_id])
    |> validate_required([:role, :team_user_id])
    |> validate_inclusion(:role, @roles)
    |> unique_constraint(:team_user_id)
  end
end
