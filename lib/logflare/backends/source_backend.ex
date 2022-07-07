defmodule Logflare.Backends.SourceBackend do
  @moduledoc false
  use TypedEctoSchema
  import Ecto.Changeset
  alias Logflare.Source

  typed_schema "source_backends" do
    belongs_to :source, Source
    field :type, Ecto.Enum, values: [:bigquery, :webhook]
    field :config, :map
    timestamps()
  end

  def changeset(source_backend, attrs) do
    source_backend
    |> cast(attrs, [:source_id, :type])
    |> validate_required([:source_id, :type])
  end
end
