defmodule Logflare.Lql.ChartRule do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset
  @derive {Jsonrs.Encoder, []}

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true, default: "timestamp"
    field :value_type, Ecto.Atom, virtual: true
    field :period, Ecto.Atom, virtual: true, default: :minute
    field :aggregate, Ecto.Atom, virtual: true, default: :count
  end

  def build_from_path(path) do
    %__MODULE__{}
    |> cast(%{path: path}, __MODULE__.__schema__(:fields))
    |> Map.get(:changes)
  end
end
