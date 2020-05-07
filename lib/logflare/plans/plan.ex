defmodule Logflare.Plans.Plan do
  use Ecto.Schema
  import Ecto.Changeset

  schema "plans" do
    field :name, :string
    field :stripe_id, :string
    field :price, :integer
    field :period, :string

    timestamps()
  end

  @doc false
  def changeset(plan, attrs) do
    plan
    |> cast(attrs, [:name, :stripe_id, :price, :period])
    |> validate_required([:name, :stripe_id, :price, :period])
  end
end
