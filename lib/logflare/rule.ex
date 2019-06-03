defmodule Logflare.Rule do
  use Ecto.Schema
  alias Logflare.Source
  import Ecto.Changeset

  schema "rules" do
    field :regex, :string
    field :sink, Ecto.UUID.Atom
    # TODO update sink field to be an belongs_to association
    # belongs_to :sink, Source, foreign_key: :sink_id, type: Ecto.UUID.Atom, references: :token
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
