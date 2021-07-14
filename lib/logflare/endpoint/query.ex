defmodule Logflare.Endpoint.Query do
  use Ecto.Schema
  import Ecto.Changeset

  schema "endpoint_queries" do
    field :token, Ecto.UUID
    field :name, :string
    field :query, :string

    belongs_to :user, Logflare.User

    timestamps()
  end

  @doc false
  def changeset(query, attrs) do
    query
    |> cast(attrs, [:name, :token, :query])
    |> validate_required([:name, :token, :query])
  end

  def update_by_user_changeset(query, attrs) do
    query
    |> cast(attrs, [
      :name,
      :token,
      :query
    ])
    |> default_validations()
  end

  def default_validations(changeset) do
    changeset
    |> validate_required([:name, :token, :query, :user])
    |> validate_query(:query)
    |> unique_constraint(:name, name: :endpoint_queries_name_index)
    |> unique_constraint(:token)
  end

  def validate_query(changeset, field) when is_atom(field) do
    validate_change(changeset, field, fn field, value ->
      case Logflare.SQL.transform(value, get_field(changeset, :user)) do
        {:ok, _} ->
          []

        {:error, error} ->
          [{field, error}]
      end
    end)
  end
end
