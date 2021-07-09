defmodule Logflare.Endpoint.Query do
  use Ecto.Schema
  import Ecto.Changeset

  schema "endpoint_queries" do
    field :token, Ecto.UUID
    field :title, :string
    field :query, :string
    field :parameter_types, :map

    belongs_to :user, Logflare.User

    timestamps()
  end

  @doc false
  def changeset(query, attrs) do
    query
    |> cast(attrs, [:title, :uuid, :query])
    |> validate_required([:title, :uuid, :query])
  end
end
