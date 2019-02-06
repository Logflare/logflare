defmodule Logflare.Rule do
  use Ecto.Schema
  import Ecto.Changeset

  schema "rules" do
    field(:regex, :string)
    field(:sink, Ecto.UUID)
    belongs_to(:source, Logflare.Source)

    timestamps()
  end

  @doc false
  def changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:regex, :sink])
    |> validate_required([:regex, :sink])
  end
end
