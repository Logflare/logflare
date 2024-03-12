defmodule Logflare.Backends.Backend do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Source
  alias Logflare.User

  @adaptor_types [:bigquery, :webhook, :postgres]

  typed_schema "backends" do
    field(:name, :string)
    field(:description, :string)
    field(:token, Ecto.UUID, autogenerate: true)
    field(:type, Ecto.Enum, values: @adaptor_types)
    # TODO: maybe use polymorphic embeds
    field(:config, :map)
    many_to_many(:sources, Source, join_through: "sources_backends")
    belongs_to(:user, User)
    timestamps()
  end

  def changeset(backend, attrs) do
    backend
    |> cast(attrs, [:type, :config, :user_id, :name, :description])
    |> validate_required([:user_id, :type, :config, :name])
    |> validate_inclusion(:type, @adaptor_types)
  end

  @spec child_spec(Source.t(), Backend.t()) :: map()
  defdelegate child_spec(source, backend), to: Adaptor

  # secrets redacting for json encoding
  defimpl Jason.Encoder, for: __MODULE__ do
    def encode(value, opts) do
      type = value.type

      values =
        Map.take(value, [
          :name,
          :token,
          :description,
          :type,
          :id,
          :config
        ])
        |> Map.update(:config, %{}, fn
          config when type == :postgres ->
            Map.put(config, :url, "redacted")

          cfg ->
            cfg
        end)

      Jason.Encode.map(values, opts)
    end
  end
end
