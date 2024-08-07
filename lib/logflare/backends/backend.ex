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
    # TODO(Ziinc): make virtual once cluster is using encrypted fields fully
    field(:config, :map)
    field(:config_encrypted, Logflare.Ecto.EncryptedMap)
    many_to_many(:sources, Source, join_through: "sources_backends")
    belongs_to(:user, User)
    has_many(:rules, Rule)
    field(:register_for_ingest, :boolean, virtual: true, default: true)
    field :metadata, :map
    timestamps()
  end

  def adaptor_mapping(), do: @adaptor_mapping

  def changeset(backend, attrs) do
    backend
    |> cast(attrs, [:type, :config, :user_id, :name, :description, :metadata])
    |> validate_required([:user_id, :type, :config, :name])
    |> validate_inclusion(:type, Map.keys(@adaptor_mapping))
    |> do_config_change()
    |> validate_config()
  end

  # temp function
  defp do_config_change(%Ecto.Changeset{changes: %{config: config}} = changeset) do
    changeset
    |> put_change(:config_encrypted, config)

    # TODO(Ziinc): uncomment once cluster is using encrypted fields fully
    # |> delete_change(:config)
  end

  defp do_config_change(changeset), do: changeset

  # common config validation function
  defp validate_config(%{valid?: true} = changeset) do
    type = Ecto.Changeset.get_field(changeset, :type)
    mod = adaptor_mapping()[type]

    Ecto.Changeset.validate_change(changeset, :config, fn :config, config ->
      case Adaptor.cast_and_validate_config(mod, config) do
        %{valid?: true} -> []
        %{valid?: false, errors: errors} -> for {key, err} <- errors, do: {:"config.#{key}", err}
      end
    end)
  end

  defp validate_config(changeset), do: changeset

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

          %{password: pass} = config when pass != nil and type == :elastic ->
            Map.put(config, :password, "REDACTED")

          cfg ->
            cfg
        end)

      Jason.Encode.map(values, opts)
    end
  end
end
