defmodule Logflare.Backends.Adaptor.SyslogAdaptor do
  @moduledoc """
  TCP backend adaptor that sends out Syslog-formatted messages.
  """

  use Supervisor
  use TypedStruct
  import Ecto.Changeset
  import NimbleParsec
  import Logflare.Logs.SyslogParser.Helpers
  alias Logflare.Backends.Adaptor.SyslogAdaptor.{Pool, Pipeline}
  @behaviour Logflare.Backends.Adaptor

  typedstruct enforce: true do
    field(:tls, boolean())
    field(:host, String.t())
    field(:port, non_neg_integer())
    field(:cipher_key, binary())
    field(:ca_cert, String.t())
    field(:client_cert, String.t())
    field(:client_key, String.t())
    field(:structured_data, String.t())
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
      {Pool, backend_id: backend.id, name: pool_name},
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
       client_key: :string,
       structured_data: :string
     }}
    |> cast(params, [
      :tls,
      :host,
      :port,
      :cipher_key,
      :ca_cert,
      :client_cert,
      :client_key,
      :structured_data
    ])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> validate_required([:host, :port])
    |> validate_inclusion(:port, 0..65_535)
    |> validate_cipher()
    |> validate_certificate(:ca_cert)
    |> validate_certificate(:client_cert)
    |> validate_private_key(:client_key)
    |> validate_structured_data()
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    config
    |> redact_config_field(:ca_cert)
    |> redact_config_field(:client_cert)
    |> redact_config_field(:client_key)
    |> redact_config_field(:cipher_key)
  end

  defp redact_config_field(config, field) do
    if Map.has_key?(config, field) do
      Map.replace!(config, field, "REDACTED")
    else
      config
    end
  end

  defp validate_cipher(changeset) do
    validate_change(changeset, :cipher_key, fn :cipher_key, key ->
      case Base.decode64(key) do
        {:ok, decoded} when byte_size(decoded) == 32 -> []
        _ -> [cipher_key: "must be a base64 encoded 32 byte key"]
      end
    end)
  end

  defp validate_certificate(changeset, field) do
    validate_change(changeset, field, fn field, value ->
      case :public_key.pem_decode(value) do
        [] ->
          [{field, "must be a valid PEM encoded string"}]

        entries ->
          Enum.flat_map(entries, fn
            {:Certificate, _der, :not_encrypted} ->
              []

            {:Certificate, _der, _cipher} ->
              [{field, "PEM entries must not be encrypted"}]

            {type, _der, _cipher} ->
              [{field, "PEM entries must all be certificates, got: #{inspect(type)}"}]
          end)
      end
    end)
  end

  @valid_key_types [:RSAPrivateKey, :ECPrivateKey, :PrivateKeyInfo]

  defp validate_private_key(changeset, field) do
    validate_change(changeset, field, fn field, value ->
      case :public_key.pem_decode(value) do
        [{type, _der, :not_encrypted}] when type in @valid_key_types ->
          []

        [{type, _der, _cipher}] when type in @valid_key_types ->
          [{field, "PEM entry must not be encrypted"}]

        [{type, _der, _cipher}] ->
          [{field, "unsupported key type: #{inspect(type)}"}]

        [_ | _] = entries ->
          [{field, "expected one PEM entry, got #{length(entries)} entries"}]

        [] ->
          [{field, "must be a valid PEM encoded string"}]
      end
    end)
  end

  defparsecp(:parse_structured_data, sd_element())

  defp validate_structured_data(changeset) do
    validate_change(changeset, :structured_data, fn :structured_data, value ->
      case parse_structured_data(value) do
        {:ok, _tokens, "", _, _, _} ->
          []

        {:ok, _tokens, _extra, _, _, _} ->
          [structured_data: "invalid format"]

        {:error, _, _, _, _, _} ->
          [structured_data: "invalid format"]
      end
    end)
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_id, _query, _opts) do
    {:error, :not_implemented}
  end
end
