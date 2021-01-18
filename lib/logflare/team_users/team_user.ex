defmodule Logflare.TeamUsers.TeamUser do
  use Ecto.Schema
  use Logflare.Commons
  import Ecto.Changeset
  use Logflare.ChangefeedSchema

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

    embeds_one :preferences, UserPreferences

    belongs_to :team, Team

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
    |> downcase_emails()
    |> downcase_email_provider_uid(team_user)
  end

  def changefeed_changeset(attrs) do
    chgst = EctoChangesetExtras.cast_all_fields(%__MODULE__{}, attrs)

    if prefs = attrs["preferences"] do
      put_embed(
        chgst,
        :preferences,
        UserPreferences.changeset(
          %UserPreferences{id: prefs["id"]},
          prefs
        )
      )
    else
      chgst
    end
  end

  def downcase_emails(changeset) do
    changeset
    |> update_change(:email, &String.downcase/1)
    |> update_change(:email_preferred, fn
      nil -> nil
      x when is_binary(x) -> String.downcase(x)
    end)
  end

  def downcase_email_provider_uid(changeset, team_user) do
    if team_user.provider == "email" do
      changeset
      |> update_change(:provider_uid, &String.downcase/1)
    else
      changeset
    end
  end
end
