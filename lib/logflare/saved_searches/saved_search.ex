defmodule Logflare.SavedSearch do
  use TypedEctoSchema
  alias Logflare.Source
  import Ecto.Changeset

  typed_schema "saved_searches" do
    field :querystring, :string
    field :saved_by_user, :boolean
    field :lql, {:array, :map}
    belongs_to :source, Source

    timestamps()
  end

  def changeset(saved_search, attrs \\ %{}) do
    saved_search
    |> cast(attrs, [:querystring, :lql, :saved_by_user])
    |> validate_required([:querystring, :lql])
    |> unique_constraint(:querystring, name: :saved_searches_querystring_source_id_index)
  end
end
