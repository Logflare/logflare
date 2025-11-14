defmodule Logflare.Backends.Adaptor.SentryAdaptor.EnvelopeBuilder do
  @moduledoc """
  A `Tesla.Middleware` sending `Logflare.LogEvent`s as [Sentry envelope](https://develop.sentry.dev/sdk/data-model/envelopes/)
  """

  alias Logflare.Sources

  @sdk_name "sentry.logflare"
  @sentry_envelope_content_type "application/x-sentry-envelope"

  @behaviour Tesla.Middleware

  @impl Tesla.Middleware
  def call(env, next, opts) do
    dsn = Keyword.fetch!(opts, :dsn)
    body = build_envelope(env.body, dsn)

    env
    |> Tesla.put_header("content-type", @sentry_envelope_content_type)
    |> Tesla.put_body(body)
    |> Tesla.run(next)
  end

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

  defp build_attributes(%Logflare.LogEvent{source_id: source_id} = log_event) do
    source = Sources.Cache.get_by_id(source_id)

    base_attrs = %{
      "sentry.sdk.name" => @sdk_name,
      "sentry.sdk.version" => Application.spec(:logflare, :vsn) |> to_string(),
      "logflare.source.name" => source.name,
      "logflare.source.service_name" => source.service_name,
      "logflare.source.uuid" => source.token
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
      v when is_map(v) or is_list(v) -> %{"value" => Jason.encode!(v), "type" => "string"}
      v -> %{"value" => inspect(v), "type" => "string"}
    end
  end
end
