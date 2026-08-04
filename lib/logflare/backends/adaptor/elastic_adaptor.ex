defmodule Logflare.Backends.Adaptor.ElasticAdaptor do
  @moduledoc """
  Ingest-only Elastic backend with a configurable transport mode.

  Supported transports:
  - `"filebeat"` (default) — Filebeat HTTP input
    https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-input-http_endpoint.html
  - `"otlp"` — OpenTelemetry Protocol HTTP/protobuf (same delivery as `OtlpAdaptor`)

  Future transports (e.g. `"logstash"`) can be added here and will use the HTTP-based pipeline
  rather than OTLP, since Logstash does not accept OTLP natively.
  """

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Utils

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @transports ["filebeat", "otlp"]
  @sensitive_headers ["authorization", "x-api-key", "x-auth-token"]

  @doc """
  Returns supported Elastic transport modes.
  """
  @spec transports() :: [String.t()]
  def transports, do: @transports

  @doc """
  Resolves the transport mode from config. Defaults to `"filebeat"` for backward compatibility.
  """
  @spec transport(map()) :: String.t()
  def transport(config) when is_map(config) do
    Map.get(config, :transport) || Map.get(config, "transport") || "filebeat"
  end

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Adaptor
  def start_link({source, backend}) do
    case transport(backend.config) do
      "otlp" ->
        HttpBased.Pipeline.start_link(source, backend, __MODULE__)

      "filebeat" ->
        backend = %{backend | config: transform_config(backend)}
        WebhookAdaptor.start_link({source, backend})
    end
  end

  @impl Adaptor
  def transform_config(%_{config: config}) do
    case transport(config) do
      "otlp" ->
        config

      "filebeat" ->
        basic_auth = Utils.encode_basic_auth(config)

        %{
          url: config.url,
          http: "http1",
          headers:
            if basic_auth do
              %{"Authorization" => "Basic #{basic_auth}"}
            else
              %{}
            end
        }
    end
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      transport: :string,
      # filebeat
      url: :string,
      username: :string,
      password: :string,
      # otlp
      endpoint: :string,
      protocol: :string,
      gzip: :boolean,
      headers: {:map, :string}
    }

    {existing_config, types}
    |> Ecto.Changeset.cast(params, Map.keys(types))
    |> Utils.default_field_value(:transport, "filebeat")
    |> Utils.default_field_value(:gzip, true)
    |> Utils.default_field_value(:protocol, "http/protobuf")
    |> Utils.default_field_value(:headers, %{})
    |> validate_user_pass()
  end

  @impl Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    transport = get_field(changeset, :transport) || "filebeat"

    changeset
    |> validate_inclusion(:transport, @transports)
    |> then(fn cs ->
      case transport do
        "otlp" ->
          cs
          |> validate_required([:endpoint])
          |> validate_format(:endpoint, ~r/https?\:\/\/.+/)
          |> validate_inclusion(:protocol, OtlpAdaptor.protocols())

        "filebeat" ->
          validate_required(cs, [:url])
      end
    end)
  end

  @impl Adaptor
  def redact_config(config) do
    config
    |> Map.replace_lazy(:password, fn _ -> "REDACTED" end)
    |> then(fn cfg ->
      if Map.has_key?(cfg, :headers) do
        Map.update!(cfg, :headers, &redact_headers/1)
      else
        cfg
      end
    end)
  end

  @impl Adaptor
  def test_connection(args) do
    config =
      case args do
        %Backend{config: config} -> config
        %{config: config} -> config
        _ -> %{}
      end

    case transport(config) do
      "otlp" -> OtlpAdaptor.Common.test_connection(__MODULE__, args)
      "filebeat" -> {:error, :not_implemented}
    end
  end

  @impl HttpBased.Client
  def client_opts(%Backend{config: config}) do
    [
      url: config.endpoint,
      formatter: ProtobufFormatter,
      gzip: config.gzip,
      json: false,
      headers: config.headers || %{}
    ]
  end

  defp validate_user_pass(changeset) do
    user = Ecto.Changeset.get_field(changeset, :username)
    pass = Ecto.Changeset.get_field(changeset, :password)
    user_pass = [user, pass]

    if user_pass != [nil, nil] and Enum.any?(user_pass, &is_nil/1) do
      msg = "Both username and password must be provided for basic auth"

      changeset
      |> Ecto.Changeset.add_error(:username, msg)
      |> Ecto.Changeset.add_error(:password, msg)
    else
      changeset
    end
  end

  defp redact_headers(headers) do
    for {k, v} <- headers, into: %{}, do: redact_header(k, v)
  end

  defp redact_header(k, v) do
    if Enum.member?(@sensitive_headers, String.downcase(k)) do
      {k, "REDACTED"}
    else
      {k, v}
    end
  end
end
