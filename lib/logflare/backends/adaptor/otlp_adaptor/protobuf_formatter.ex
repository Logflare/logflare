defmodule Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter do
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.KeyValueList
  alias Opentelemetry.Proto.Logs.V1.LogRecord
  alias Opentelemetry.Proto.Logs.V1.ResourceLogs
  alias Opentelemetry.Proto.Logs.V1.ScopeLogs
  alias Opentelemetry.Proto.Logs.V1.SeverityNumber
  alias Opentelemetry.Proto.Resource.V1.Resource
  alias Logflare.LogEvent

  @behaviour Tesla.Middleware

  @protobuf_content_type "application/x-protobuf"

  @impl true
  def call(env, next, opts) do
    metadata = opts[:metadata] || %{}

    response =
      env
      |> Tesla.put_header("content-type", @protobuf_content_type)
      |> Tesla.put_body(transform_batch(env.body, metadata))
      |> Tesla.run(next)

    with {:ok, %{status: 200} = env} <- response do
      if Tesla.get_header(env, "content-type") == @protobuf_content_type do
        response = Protobuf.decode(env.body, ExportLogsServiceResponse)
        {:ok, Tesla.put_body(env, response)}
      else
        response
      end
    end
  end

  defp transform_batch(events, metadata) do
    attributes = for {k, v} <- metadata, do: {"logflare.#{k}", v}

    %ExportLogsServiceRequest{
      resource_logs: [
        %ResourceLogs{
          resource: build_resource(attributes),
          scope_logs: build_scope_logs(events),
          schema_url: "https://opentelemetry.io/schemas/1.26.0"
        }
      ]
    }
    |> Protobuf.encode_to_iodata()
  end

  defp build_resource(kv) do
    %Resource{attributes: Enum.map(kv, &make_key_value/1)}
  end

  defp build_scope_logs(logs) do
    [
      %ScopeLogs{
        scope: %InstrumentationScope{name: "logflare"},
        log_records: Enum.map(logs, &build_log_record/1)
      }
    ]
  end

  defp build_log_record(%LogEvent{} = ev) do
    observed_ts = (ev.ingested_at || 0) |> Timex.to_datetime() |> DateTime.to_unix(:nanosecond)

    {known_entries, body} =
      Map.split(ev.body, [
        "timestamp",
        "event_message",
        "attributes",
        "trace_id",
        "span_id",
        "severity_number",
        "severity_text"
      ])

    fields = Enum.flat_map(known_entries, &build_log_record_fields/1)

    struct!(LogRecord, [observed_time_unix_nano: observed_ts, body: make_value(body)] ++ fields)
  end

  defp build_log_record_fields({"timestamp", ts}),
    do: [time_unix_nano: System.convert_time_unit(ts, :microsecond, :nanosecond)]

  defp build_log_record_fields({"event_message", msg}) when is_binary(msg),
    do: [event_name: msg]

  defp build_log_record_fields({"attributes", attrs}) when is_map(attrs),
    do: [attributes: Enum.map(attrs, &make_key_value/1)]

  defp build_log_record_fields({"severity_number", number}) when is_integer(number),
    do: [severity_number: SeverityNumber.key(number)]

  defp build_log_record_fields({"severity_text", msg}) when is_binary(msg),
    do: [severity_text: msg]

  defp build_log_record_fields({"trace_id", id}) when is_binary(id), do: [trace_id: id]
  defp build_log_record_fields({"span_id", id}) when is_binary(id), do: [span_id: id]
  defp build_log_record_fields(_unmatched), do: []

  defp make_value(v) when is_binary(v), do: %AnyValue{value: {:string_value, v}}
  defp make_value(v) when is_boolean(v), do: %AnyValue{value: {:bool_value, v}}
  defp make_value(v) when is_integer(v), do: %AnyValue{value: {:int_value, v}}
  defp make_value(v) when is_float(v), do: %AnyValue{value: {:double_value, v}}
  defp make_value(v) when is_list(v), do: %AnyValue{value: {:array_value, make_array(v)}}
  defp make_value(v) when is_map(v), do: %AnyValue{value: {:kvlist_value, make_key_value_list(v)}}
  # TODO: distinguish from string
  defp make_value(v) when is_binary(v), do: %AnyValue{value: {:bytes_value, v}}

  defp make_key_value({k, v}), do: %KeyValue{key: to_string(k), value: make_value(v)}

  defp make_key_value_list(kv) do
    %KeyValueList{values: Enum.map(kv, &make_key_value/1)}
  end

  defp make_array(enum) do
    %ArrayValue{values: Enum.map(enum, &make_value/1)}
  end
end
