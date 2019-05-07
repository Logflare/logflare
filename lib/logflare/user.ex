defmodule Logflare.User do
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field(:email, :string)
    field(:provider, :string)
    field(:token, :string)
    field(:api_key, :string)
    field(:old_api_key, :string)
    field(:email_preferred, :string)
    field(:name, :string)
    field(:image, :string)
    field(:email_me_product, :boolean)
    field(:admin, :boolean)
    has_many(:sources, Logflare.Source)
    field(:phone, :string)
    field(:bigquery_project_id, :string)

    timestamps()
  end

  @doc false
  def changeset(user, attrs) do
    user
    |> cast(attrs, [
      :email,
      :provider,
      :token,
      :api_key,
      :old_api_key,
      :email_preferred,
      :name,
      :image,
      :email_me_product,
      :admin,
      :phone,
      :bigquery_project_id
    ])
    |> validate_required([:email, :provider, :token])
  end
end
