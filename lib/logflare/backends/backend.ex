defmodule Logflare.Backends.Backend do
  @moduledoc false

  use TypedEctoSchema
  import Ecto.Changeset

  alias Ecto.Changeset
  alias Logflare.Alerting.AlertQuery
  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Backend
  alias Logflare.Endpoints.Query
  alias Logflare.Rules.Rule
  alias Logflare.Source
  alias Logflare.User

  @adaptor_mapping %{
    webhook: Adaptor.WebhookAdaptor,
    elastic: Adaptor.ElasticAdaptor,
    datadog: Adaptor.DatadogAdaptor,
    postgres: Adaptor.PostgresAdaptor,
    bigquery: Adaptor.BigQueryAdaptor,
    loki: Adaptor.LokiAdaptor,
    clickhouse: Adaptor.ClickhouseAdaptor,
    incidentio: Adaptor.IncidentioAdaptor,
    s3: Adaptor.S3Adaptor
  }

  typed_schema "backends" do
    field(:name, :string)
    field(:description, :string)
    field(:token, Ecto.UUID, autogenerate: true)
    field(:type, Ecto.Enum, values: Map.keys(@adaptor_mapping))
    field(:config, :map, virtual: true)
    field(:config_encrypted, Logflare.Ecto.EncryptedMap)
    many_to_many(:sources, Source, join_through: "sources_backends")
    belongs_to(:user, User)
    has_many(:rules, Rule)

    many_to_many(:alert_queries, AlertQuery,
      join_through: "alert_queries_backends",
      on_replace: :delete
    )

    has_many(:endpoint_queries, Query)

    field(:register_for_ingest, :boolean, virtual: true, default: true)
    field :metadata, :map
    field :default_ingest?, :boolean, source: :default_ingest, default: false
    timestamps()
  end

  def adaptor_mapping(), do: @adaptor_mapping

  def changeset(backend, attrs) do
    backend
    |> cast(attrs, [:type, :config, :user_id, :name, :description, :metadata, :default_ingest?])
    |> validate_required([:user_id, :type, :config, :name])
    |> validate_inclusion(:type, Map.keys(@adaptor_mapping))
    |> validate_config()
    |> validate_default_ingest()
    |> do_config_change()
  end

  # temp function
  defp do_config_change(%Changeset{changes: %{config: config}} = changeset) do
    changeset
    |> put_change(:config_encrypted, config)
    |> delete_change(:config)
  end

  defp do_config_change(changeset), do: changeset

  # common config validation function
  defp validate_config(%{valid?: true} = changeset) do
    type = Changeset.get_field(changeset, :type)
    mod = adaptor_mapping()[type]

    Changeset.validate_change(changeset, :config, fn :config, config ->
      case Adaptor.cast_and_validate_config(mod, config) do
        %{valid?: true} -> []
        %{valid?: false, errors: errors} -> for {key, err} <- errors, do: {:"config.#{key}", err}
      end
    end)
  end

  defp validate_config(changeset), do: changeset

  defp validate_default_ingest(%Changeset{changes: %{default_ingest?: true}} = changeset) do
    type = get_field(changeset, :type)
    backend = %Backend{type: type}

    if Adaptor.supports_default_ingest?(backend) do
      changeset
    else
      add_error(
        changeset,
        :default_ingest?,
        "Backend type #{type} does not support default ingest"
      )
    end
  end

  defp validate_default_ingest(changeset), do: changeset

  @spec child_spec(Source.t(), Backend.t()) :: map()
  defdelegate child_spec(source, backend), to: Adaptor

  # secrets redacting for json encoding
  defimpl Jason.Encoder, for: __MODULE__ do
    def encode(value, opts) do
      type = value.type

      values =
        value
        |> Map.put(:config, value.config_encrypted)
        |> Map.take([
          :name,
          :token,
          :description,
          :type,
          :id,
          :config,
          :metadata
        ])
        |> Map.update(:config, %{}, fn
          config when type == :postgres ->
            url = Map.get(config, :url) || Map.get(config, "url")
            updated = String.replace(url, ~r/(.+):.+\@/, "\\g{1}:REDACTED@")
            Map.put(config, :url, updated)

          config when type == :datadog ->
            Map.put(config, :api_key, "REDACTED")

          %{password: pass} = config when pass != nil ->
            Map.put(config, :password, "REDACTED")

          cfg ->
            cfg
        end)

      Jason.Encode.map(values, opts)
    end
  end
end
