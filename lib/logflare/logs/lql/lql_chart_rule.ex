defmodule Logfalre.Lql.ChartRule do
  @moduledoc false
  use Ecto.Schema
  import Ecto.Changeset

  @primary_key false
  embedded_schema do
    field :path, :string, virtual: true
  end

  def build_from_path(path) do
    %__MODULE__{}
    |> cast(%{path: path}, __MODULE__.__schema__(:fields))
    |> Map.get(:changes)
  end
end
