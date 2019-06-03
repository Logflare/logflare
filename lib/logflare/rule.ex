defmodule Logflare.Rule do
  use Ecto.Schema
  alias Logflare.Source
  import Ecto.Changeset

  schema "rules" do
    field :regex, :string
    belongs_to :sink, Source
    belongs_to :source, Source

    timestamps()
  end

  @doc false
  def changeset(rule, attrs \\ %{}) do
    rule
    |> cast(attrs, [:regex, :sink])
    |> validate_required([:regex, :sink])
  end
end
