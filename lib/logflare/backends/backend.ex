defmodule Logflare.Backends.Backend do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset
  alias Logflare.Source

  typed_schema "backends" do
    belongs_to :source, Source
    field :type, :string
    field :config, :map

    timestamps()
  end

  def changeset(backend, attrs) do
    backend
    |> cast(attrs, [:source_id, :type])
    |> validate_required([:type, :config])
  end
end
