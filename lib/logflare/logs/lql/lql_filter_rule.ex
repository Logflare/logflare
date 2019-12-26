defmodule Logflare.Lql.FilterRule do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true
    field :operator, Ecto.Atom, virtual: true
    field :value, :any, virtual: true
    field :modifiers, {:array, :any}, virtual: true, default: []
  end

  def build(params) when is_list(params) do
    %__MODULE__{}
    |> cast(Map.new(params), __MODULE__.__schema__(:fields))
    |> Map.get(:changes)
  end
end
