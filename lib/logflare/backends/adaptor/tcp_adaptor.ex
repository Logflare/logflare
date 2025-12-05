defmodule Logflare.Backends.Adaptor.TCPAdaptor do
  @moduledoc """
  TCP backend adaptor that sends out Syslog-formatted messages.
  """

  use Supervisor
  use TypedStruct
  import Ecto.Changeset
  alias Logflare.Backends.Adaptor.TCPAdaptor.{Pool, Pipeline}
  @behaviour Logflare.Backends.Adaptor

  typedstruct enforce: true do
    field(:tls, boolean())
    field(:host, String.t())
    field(:port, non_neg_integer())
    field(:cipher_key, binary())
    field(:ca_cert, String.t())
    field(:client_cert, String.t())
    field(:client_key, String.t())
  end

  @impl Logflare.Backends.Adaptor
  def supports_default_ingest?, do: true

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    name = Logflare.Backends.via_source(source, __MODULE__, backend)
    Supervisor.start_link(__MODULE__, {source, backend}, name: name)
  end

  @impl Supervisor
  def init({source, backend}) do
    pool_name = Logflare.Backends.via_source(source, Pool, backend)
    pipeline_name = Logflare.Backends.via_source(source, Pipeline, backend)

    children = [
      {Pool, config: backend.config, name: pool_name},
      {Pipeline, source: source, backend: backend, pool: pool_name, name: pipeline_name}
    ]

    Supervisor.init(children, strategy: :rest_for_one)
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{},
     %{
       tls: :boolean,
       host: :string,
       port: :integer,
       cipher_key: :string,
       ca_cert: :string,
       client_cert: :string,
       client_key: :string
     }}
    |> cast(params, [:tls, :host, :port, :cipher_key, :ca_cert, :client_cert, :client_key])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> validate_inclusion(:port, 0..65_535)
    |> validate_change(:cipher_key, fn :cipher_key, key ->
      case Base.decode64(key) do
        {:ok, decoded} when byte_size(decoded) == 32 -> []
        _ -> [cipher_key: "must be a base64 encoded 32 byte key"]
      end
    end)
    |> validate_change(:ca_cert, &validate_pem/2)
    |> validate_change(:client_cert, &validate_pem/2)
    |> validate_change(:client_key, &validate_pem/2)
  end

  defp validate_pem(field, value) do
    case :public_key.pem_decode(value) do
      [] -> [{field, "must be a valid PEM encoded string"}]
      _ -> []
    end
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query, _opts) do
    {:error, :not_implemented}
  end
end
