defmodule Logflare.Backends.SourceBackend do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.SourceBackend
  alias Logflare.Source

  typed_schema "source_backends" do
    belongs_to(:source, Source)
    field(:type, Ecto.Enum, values: [:bigquery, :webhook, :postgres])
    field(:config, :map)
    timestamps()
  end

  def changeset(source_backend, attrs) do
    source_backend
    |> cast(attrs, [:source_id, :type, :config])
    |> validate_required([:source_id, :type, :config])
  end

  @spec child_spec(SourceBackend.t()) :: map()
  defdelegate child_spec(source_backend), to: Adaptor
end
