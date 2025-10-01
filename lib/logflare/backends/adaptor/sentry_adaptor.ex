defmodule Logflare.Backends.Adaptor.SentryAdaptor do
  @sdk_name "sentry.logflare"
  @sentry_envelope_content_type "application/x-sentry-envelope"

  @moduledoc """
  Sentry adaptor for sending logs to Sentry's logging API.

  This adaptor wraps the WebhookAdaptor to provide specific functionality
  for sending logs to Sentry in the expected envelope format.

  ## Configuration

  The adaptor requires a single configuration parameter:

  - `dsn` - The Sentry DSN string in the format:
    `{PROTOCOL}://{PUBLIC_KEY}:{SECRET_KEY}@{HOST}{PATH}/{PROJECT_ID}`

  ## Example DSN

      https://abc123@o123456.ingest.sentry.io/123456
  """

  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Adaptor.SentryAdaptor.DSN

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{
      id: __MODULE__,
      start: {__MODULE__, :start_link, [arg]}
    }
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(%{config: config}) do
    case DSN.parse(config.dsn) do
      {:ok, parsed_dsn} ->
        %{
          url: parsed_dsn.endpoint_uri,
          headers: %{"content-type" => @sentry_envelope_content_type},
          http: "http2",
          format_batch: fn log_events ->
            build_envelope(log_events, parsed_dsn.original_dsn)
          end
        }

      {:error, reason} ->
        raise ArgumentError, "Invalid Sentry DSN: #{reason}"
    end
  end

  @impl Logflare.Backends.Adaptor
  def execute_query(_ident, _query, _opts), do: {:error, :not_implemented}

  @impl Logflare.Backends.Adaptor
  def cast_config(params) do
    {%{}, %{dsn: :string}}
    |> Ecto.Changeset.cast(params, [:dsn])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:dsn])
    |> validate_dsn()
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    case Map.get(config, :dsn) || Map.get(config, "dsn") do
      nil ->
        config

      dsn ->
        redacted_dsn = String.replace(dsn, ~r/\/\/([^:]+):[^@]+@/, "//\\1:REDACTED@")
        Map.put(config, :dsn, redacted_dsn)
    end
  end

  defp validate_dsn(%{changes: %{dsn: dsn}} = changeset) do
    case DSN.parse(dsn) do
      {:ok, _parsed_dsn} ->
        changeset

      {:error, reason} ->
        Ecto.Changeset.add_error(changeset, :dsn, "Invalid DSN: #{reason}")
    end
  end

  defp validate_dsn(changeset), do: changeset

  # Based on https://develop.sentry.dev/sdk/data-model/envelopes/
  defp build_envelope(log_events, original_dsn) do
    sentry_logs = Enum.map(log_events, &translate_log_event/1)

    header = %{
      "dsn" => original_dsn,
      "sent_at" => DateTime.utc_now() |> DateTime.to_iso8601(:extended)
    }

    item_header = %{
      "type" => "log",
      "item_count" => length(sentry_logs),
      "content_type" => "application/vnd.sentry.items.log+json"
    }

    item_payload = %{"items" => sentry_logs}

    Enum.join(
      [
        Jason.encode!(header),
        Jason.encode!(item_header),
        Jason.encode!(item_payload)
      ],
      "\n"
    )
  end

  # https://develop.sentry.dev/sdk/telemetry/logs/#log-envelope-item-payload
  defp translate_log_event(%Logflare.LogEvent{} = log_event) do
    timestamp_seconds = log_event.body["timestamp"] / 1_000_000

    message = log_event.body["event_message"] || ""

    level =
      case log_event.body["level"] do
        nil -> "info"
        level when is_binary(level) -> normalize_level(level)
        level when is_atom(level) -> normalize_level(Atom.to_string(level))
        _ -> "info"
      end

    %{
      "timestamp" => timestamp_seconds,
      "level" => level,
      "body" => message,
      "trace_id" => extract_trace_id(log_event),
      "attributes" => build_attributes(log_event)
    }
  end

  defp build_attributes(%Logflare.LogEvent{} = log_event) do
    base_attrs = %{
      "sentry.sdk.name" => @sdk_name,
      "sentry.sdk.version" => Application.spec(:logflare, :vsn) |> to_string(),
      "logflare.source.name" => log_event.source.name,
      "logflare.source.service_name" => log_event.source.service_name,
      "logflare.source.uuid" => log_event.source.token
    }

    top_level_attrs =
      log_event.body
      |> Map.drop([
        "timestamp",
        "event_message",
        "level",
        "trace_id",
        "trace.id"
      ])

    base_attrs
    |> Map.merge(top_level_attrs)
    |> Enum.filter(fn {_k, v} -> v != nil end)
    |> Map.new(fn {k, v} -> {k, to_sentry_value(v)} end)
  end

  defp extract_trace_id(%Logflare.LogEvent{} = log_event) do
    case log_event.body["trace_id"] || log_event.body["trace.id"] do
      nil ->
        generate_trace_id()

      trace_id when is_binary(trace_id) ->
        trace_id

      trace_id ->
        to_string(trace_id)
    end
  end

  defp generate_trace_id do
    # Generate a 32-character hex string (128 bits) as a fake trace ID
    :crypto.strong_rand_bytes(16)
    |> Base.encode16(case: :lower)
  end

  defp normalize_level(level_string) do
    case String.downcase(level_string) do
      "debug" -> "debug"
      "info" -> "info"
      "notice" -> "info"
      "warning" -> "warn"
      "warn" -> "warn"
      "error" -> "error"
      "fatal" -> "fatal"
      "critical" -> "fatal"
      "alert" -> "fatal"
      "emergency" -> "fatal"
      _ -> "info"
    end
  end

  defp to_sentry_value(value) do
    case value do
      nil -> %{"value" => "", "type" => "string"}
      v when is_binary(v) -> %{"value" => v, "type" => "string"}
      v when is_boolean(v) -> %{"value" => v, "type" => "boolean"}
      v when is_integer(v) -> %{"value" => v, "type" => "integer"}
      v when is_float(v) -> %{"value" => v, "type" => "double"}
      v -> %{"value" => inspect(v), "type" => "string"}
    end
  end
end
