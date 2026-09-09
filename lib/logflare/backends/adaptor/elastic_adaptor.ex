defmodule Logflare.Backends.Adaptor.ElasticAdaptor do
  @moduledoc """
  Ingest-only Elastic backend with a configurable transport mode.

  Supported transports:
  - `"filebeat"` (default) — Filebeat HTTP input
    https://www.elastic.co/guide/en/beats/filebeat/current/filebeat-input-http_endpoint.html
  - `"logstash"` — Logstash `http` input plugin, receiving ECS-flavoured JSON
    https://www.elastic.co/guide/en/logstash/current/plugins-inputs-http.html
  - `"otlp"` — OpenTelemetry Protocol HTTP/protobuf (same delivery as `OtlpAdaptor`)

  `"filebeat"` and `"logstash"` share the `WebhookAdaptor` HTTP pipeline; Logstash does not accept
  OTLP natively, so it reshapes each event into an ECS-flavoured document instead.
  """

  import Logflare.Utils.Guards, only: [is_pos_integer: 1]

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.OtlpAdaptor
  alias Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Utils

  @behaviour Adaptor
  @behaviour HttpBased.Client

  @transports ["filebeat", "logstash", "otlp"]
  @webhook_transports ["filebeat", "logstash"]
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

      transport when transport in @webhook_transports ->
        backend = %{backend | config: transform_config(backend)}
        WebhookAdaptor.start_link({source, backend})
    end
  end

  @impl Adaptor
  def transform_config(%_{config: config}) do
    case transport(config) do
      "otlp" ->
        config

      "logstash" ->
        %{
          url: config.url,
          http: "http1",
          gzip: Map.get(config, :gzip, true),
          headers: headers_with_basic_auth(config),
          format_batch: &format_batch/1
        }

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

  @doc """
  Reshapes a batch into ECS-flavoured documents for the Logstash `http` input.

  `timestamp` and `event_message` are lifted to the ECS `@timestamp` and `message` fields so that a
  receiving Logstash pipeline needs no `date` filter. Remaining body fields stay at the top level;
  Logflare-specific metadata is namespaced under `logflare`.
  """
  @impl Adaptor
  @spec format_batch([LogEvent.t()]) :: [map()]
  def format_batch(log_events) do
    Enum.map(log_events, &format_event/1)
  end

  @impl Adaptor
  def redact_config(config) do
    Map.replace_lazy(config, :password, fn _ -> "REDACTED" end)
  end

  @impl Adaptor
  def sanitize_config_for_display(config) do
    Adaptor.mask_config_values(config, except: [:url])
  end

  @impl Adaptor
  def cast_config(params, existing_config \\ %{}) do
    types = %{
      transport: :string,
      # filebeat, logstash
      url: :string,
      username: :string,
      password: :string,
      # logstash, otlp
      gzip: :boolean,
      headers: {:map, :string},
      # otlp
      endpoint: :string,
      protocol: :string
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

        "logstash" ->
          cs
          |> validate_required([:url])
          |> validate_format(:url, ~r/https?\:\/\/.+/)

        "filebeat" ->
          validate_required(cs, [:url])

        _unsupported ->
          cs
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

    case {transport(config), args} do
      {"otlp", _args} ->
        OtlpAdaptor.Common.test_connection(__MODULE__, args)

      {"logstash", %Backend{} = backend} ->
        WebhookAdaptor.test_connection(%{backend | config: transform_config(backend)}, [])

      {_transport, _args} ->
        {:error, :not_implemented}
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

  defp format_event(%LogEvent{body: body} = log_event) do
    {timestamp, body} = Map.pop(body, "timestamp")
    {message, body} = Map.pop(body, "event_message")
    {id, body} = Map.pop(body, "id")

    body
    |> Map.put("logflare", %{
      "id" => id || log_event.id,
      "source" => log_event.source_name,
      "source_uuid" => log_event.source_uuid && to_string(log_event.source_uuid),
      "event_type" => log_event.event_type && to_string(log_event.event_type)
    })
    |> put_timestamp(timestamp, log_event.ingested_at)
    |> put_message(message)
  end

  defp put_timestamp(payload, timestamp, _ingested_at) when is_pos_integer(timestamp) do
    Map.put(
      payload,
      "@timestamp",
      timestamp |> DateTime.from_unix!(:microsecond) |> DateTime.to_iso8601()
    )
  end

  defp put_timestamp(payload, _timestamp, %DateTime{} = ingested_at) do
    Map.put(payload, "@timestamp", DateTime.to_iso8601(ingested_at))
  end

  defp put_timestamp(payload, _timestamp, _ingested_at), do: payload

  defp put_message(payload, message) when is_binary(message),
    do: Map.put(payload, "message", message)

  defp put_message(payload, _message), do: payload

  defp headers_with_basic_auth(config) do
    headers = Map.get(config, :headers) || %{}

    case Utils.encode_basic_auth(config) do
      nil -> headers
      basic_auth -> Map.put(headers, "Authorization", "Basic #{basic_auth}")
    end
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
