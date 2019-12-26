defmodule LogflareWeb.Source.SearchLV.SearchParams do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset

  typed_embedded_schema do
    field :tailing?, :boolean
    field :querystring, :string
    field :search_chart_aggregate, Ecto.Atom
    field :search_chart_period, Ecto.Atom
  end

  def new(params) do
    %__MODULE__{}
    |> cast(params, __schema__(:fields))
    |> Map.get(:changes)
  end
end
