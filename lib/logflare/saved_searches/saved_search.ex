defmodule Logflare.SavedSearch do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Sources.Source
  import Ecto.Changeset

  typed_schema "saved_searches" do
    field :name, :string
    field :description, :string
    field :querystring, :string
    field :saved_by_user, :boolean, default: false
    field :favorite, :boolean, default: false
    field :lql_filters, {:array, :map}, default: []
    field :lql_charts, {:array, :map}, default: []
    field :tailing, :boolean, default: true
    belongs_to :source, Source

    timestamps()
  end

  def changeset(saved_search, attrs \\ %{}) do
    changeset =
      cast(saved_search, attrs, [
        :name,
        :description,
        :querystring,
        :lql_filters,
        :lql_charts,
        :saved_by_user,
        :favorite,
        :tailing
      ])
      |> validate_required([:querystring, :lql_filters, :lql_charts, :tailing])
      |> validate_length(:name, max: 255, count: :codepoints)
      |> unique_constraint(:querystring, name: :saved_searches_querystring_source_id_index)

    if get_field(changeset, :favorite) do
      validate_required(changeset, [:name], message: "Can’t be blank when a favorite")
    else
      changeset
    end
  end
end
