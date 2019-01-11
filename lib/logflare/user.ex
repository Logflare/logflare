defmodule Logflare.User do
  use Ecto.Schema
  import Ecto.Changeset

  schema "users" do
    field :email, :string
    field :provider, :string
    field :token, :string
    field :api_key, :string
    field :old_api_key, :string
    has_many :sources, Logflare.Source

    timestamps()
  end

  @doc false
  def changeset(user, attrs) do
    user
    |> cast(attrs, [:email, :provider, :token, :api_key, :old_api_key])
    |> validate_required([:email, :provider, :token, :api_key])
  end
end
