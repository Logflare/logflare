defmodule Logflare.Backends.Backend do
  @moduledoc false
  use TypedEctoSchema

  import Ecto.Changeset

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Source
  alias Logflare.User
  alias Logflare.Rule

  @adaptor_mapping %{
    webhook: Adaptor.WebhookAdaptor,
    elastic: Adaptor.ElasticAdaptor,
    datadog: Adaptor.DatadogAdaptor,
    postgres: Adaptor.PostgresAdaptor,
    bigquery: Adaptor.BigQueryAdaptor
  }

  typed_schema "backends" do
    field(:name, :string)
    field(:description, :string)
    field(:token, Ecto.UUID, autogenerate: true)
    field(:type, Ecto.Enum, values: Map.keys(@adaptor_mapping))
    # TODO: maybe use polymorphic embeds
    field(:config, :map)
    many_to_many(:sources, Source, join_through: "sources_backends")
    belongs_to(:user, User)
    has_many(:rules, Rule)
    field(:register_for_ingest, :boolean, virtual: true, default: true)
    timestamps()
  end

  def adaptor_mapping(), do: @adaptor_mapping

  def changeset(backend, attrs) do
    backend
    |> cast(attrs, [:type, :config, :user_id, :name, :description])
    |> validate_required([:user_id, :type, :config, :name])
    |> validate_inclusion(:type, Map.keys(@adaptor_mapping))
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
            url = Map.get(config, :url) || Map.get(config, "url")
            updated = String.replace(url, ~r/(.+):.+\@/, "\\g{1}:REDACTED@")
            Map.put(config, :url, updated)

          cfg ->
            cfg
        end)

      Jason.Encode.map(values, opts)
    end
  end
end
