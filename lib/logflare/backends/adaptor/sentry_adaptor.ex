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
            case log_events do
              # If there are no log events, don't build an envelope
              [] -> nil
              _ -> build_envelope(log_events, parsed_dsn.original_dsn)
            end
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

  defp validate_dsn(changeset) do
    case Ecto.Changeset.get_field(changeset, :dsn) do
      nil ->
        changeset

      dsn ->
        case DSN.parse(dsn) do
          {:ok, _parsed_dsn} ->
            changeset

          {:error, reason} ->
            Ecto.Changeset.add_error(changeset, :dsn, "Invalid DSN: #{reason}")
        end
    end
  end

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

  defp translate_log_event(%Logflare.LogEvent{} = log_event) do
    # Convert microsecond timestamp to seconds (with fractional part)
    # Use timestamp from outer body if available, fallback to nested body
    timestamp_seconds = log_event.body["timestamp"] / 1_000_000

    # Extract message from the log event
    message = log_event.body["event_message"] || ""

    # Determine log level - check both body and nested body
    level =
      case log_event.body["level"] do
        nil -> "info"
        level when is_binary(level) -> normalize_level(level)
        level when is_atom(level) -> normalize_level(Atom.to_string(level))
        _ -> "info"
      end

    # Extract or generate trace_id
    trace_id = extract_trace_id(log_event)

    # Build attributes from log event metadata
    attributes = build_attributes(log_event)

    %{
      "timestamp" => timestamp_seconds,
      "level" => level,
      "body" => message,
      "trace_id" => trace_id,
      "attributes" => attributes
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
        "trace.id",
      ])

    base_attrs
    |> Map.merge(top_level_attrs)
    |> Enum.filter(fn {_k, v} -> v != nil end)
    |> Map.new(fn {k, v} -> {k, to_sentry_value(v)} end)
  end

  defp valid_trace_id?(trace_id) when is_binary(trace_id) do
    # Valid trace ID should be 32 characters (128 bits) hex string
    # and not be all zeros
    with true <- String.length(trace_id) == 32,
         true <- String.match?(trace_id, ~r/^[0-9a-fA-F]+$/),
         false <- trace_id == String.duplicate("0", 32) do
      true
    else
      _ -> false
    end
  end

  defp valid_trace_id?(_), do: false

  defp extract_trace_id(%Logflare.LogEvent{} = log_event) do
    case log_event.body["trace_id"] || log_event.body["trace.id"] do
      nil ->
        generate_trace_id()

      trace_id when is_binary(trace_id) ->
        if valid_trace_id?(trace_id), do: trace_id, else: generate_trace_id()

      trace_id ->
        trace_id_string = to_string(trace_id)
        if valid_trace_id?(trace_id_string), do: trace_id_string, else: generate_trace_id()
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
