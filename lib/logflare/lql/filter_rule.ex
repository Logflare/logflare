defmodule Logflare.Lql.FilterRule do
  @moduledoc false

  use TypedEctoSchema
  import Ecto.Changeset

  @derive {Jason.Encoder, []}

  @primary_key false
  typed_embedded_schema do
    field :path, :string, virtual: true
    field :operator, Ecto.Atom, virtual: true
    field :value, :any, virtual: true
    field :values, {:array, :any}, virtual: true
    field :modifiers, {:map, Ecto.Atom}, virtual: true, default: %{}
    field :shorthand, :string, virtual: true
  end

  @spec changeset(any(), __MODULE__.t()) :: Ecto.Changeset.t()
  def changeset(_, %__MODULE__{} = rule) do
    cast(rule, %{}, fields())
  end

  @spec changeset(__MODULE__.t(), map()) :: Ecto.Changeset.t()
  def changeset(rule, params) do
    cast(rule, params, fields())
  end

  @spec build(list()) :: __MODULE__.t()
  def build(params) when is_list(params) do
    %__MODULE__{}
    |> cast(Map.new(params), fields())
    |> Map.get(:changes)
  end

  @spec fields() :: list(atom())
  def fields() do
    __MODULE__.__schema__(:fields)
  end
end
