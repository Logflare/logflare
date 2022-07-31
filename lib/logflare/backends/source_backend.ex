defmodule Logflare.Backends.SourceBackend do
  @moduledoc false
  use TypedEctoSchema
  alias Logflare.Backends.SourceBackend
  import Ecto.Changeset
  alias Logflare.Source
  alias Logflare.Backends.Adaptor.WebhookAdaptor

  typed_schema "source_backends" do
    belongs_to :source, Source
    field :type, Ecto.Enum, values: [:bigquery, :webhook]
    field :config, :map
    timestamps()
  end

  def changeset(source_backend, attrs) do
    source_backend
    |> cast(attrs, [:source_id, :type, :config])
    |> validate_required([:source_id, :type, :config])
  end

  @spec child_spec(SourceBackend.t()) :: map()
  def child_spec(%__MODULE__{type: type} = source_backend) do
    adaptor_module =
      case type do
        :webhook -> WebhookAdaptor
      end

    %{
      id: child_spec_id(source_backend),
      start: {adaptor_module, :start_link, [source_backend]}
    }
  end

  @spec child_spec_id(SourceBackend.t()) :: String.t()
  def child_spec_id(source_backend), do: "source-backend-#{source_backend.id}"
end
