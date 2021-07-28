defmodule Logflare.Vercel.Auth do
  use Ecto.Schema
  import Ecto.Changeset

  alias Logflare.User

  schema "vercel_auths" do
    field :access_token, :string
    field :installation_id, :string
    field :team_id, :string
    field :token_type, :string
    field :vercel_user_id, :string

    belongs_to :user, User

    timestamps()
  end

  @doc false
  def changeset(auth, attrs) do
    auth
    |> cast(attrs, [:access_token, :installation_id, :team_id, :token_type, :vercel_user_id])
    |> validate_required([:access_token, :installation_id, :token_type, :vercel_user_id])
  end
end
