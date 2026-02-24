defmodule Logflare.SavedSearch do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Sources.Source
  import Ecto.Changeset

  typed_schema "saved_searches" do
    field :querystring, :string
    field :saved_by_user, :boolean, default: false
    field :lql_filters, {:array, :map}, default: []
    field :lql_charts, {:array, :map}, default: []
    field :tailing, :boolean, default: true
    belongs_to :source, Source

    timestamps()
  end

  def changeset(saved_search, attrs \\ %{}) do
    saved_search
    |> cast(attrs, [:querystring, :lql_filters, :lql_charts, :saved_by_user, :tailing])
    |> validate_required([:querystring, :lql_filters, :lql_charts, :tailing])
    |> unique_constraint(:querystring, name: :saved_searches_querystring_source_id_index)
  end
end
