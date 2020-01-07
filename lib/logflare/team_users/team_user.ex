defmodule Logflare.TeamUsers.TeamUser do
  use Ecto.Schema
  import Ecto.Changeset

  schema "team_users" do
    field :email, :string
    field :email_me_product, :boolean, default: true
    field :email_preferred, :string
    field :image, :string
    field :name, :string
    field :phone, :string
    field :provider, :string
    field :provider_uid, :string
    field :token, :string
    field :valid_google_account, :boolean
    belongs_to :team, Logflare.Teams.Team

    timestamps()
  end

  @doc false
  def changeset(team_user, attrs) do
    team_user
    |> cast(attrs, [
      :email,
      :token,
      :provider,
      :email_preferred,
      :name,
      :image,
      :email_me_product,
      :phone,
      :valid_google_account,
      :provider_uid
    ])
    |> validate_required([:email, :provider, :token, :provider_uid])
  end
end
