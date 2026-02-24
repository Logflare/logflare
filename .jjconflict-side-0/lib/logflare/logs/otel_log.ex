defmodule Logflare.Logs.OtelLog do
  @moduledoc """
  Converts a list of Otel ResourceLogs to a list of Logflare events

  * One ResourceLog can contain multiple ScopeLogs
  * One ScopeLogs can contain multiple LogRecords
  * A Log Event is created for each LogRecord

  `event_message` is derived from the LogRecord body whenever the log body is a
  string. When it is something else, it is derived from the log `event_name`.
  """

  alias Logflare.Logs.Otel

  alias Opentelemetry.Proto.Logs.V1.ResourceLogs
  alias Opentelemetry.Proto.Logs.V1.ScopeLogs
  alias Opentelemetry.Proto.Logs.V1.LogRecord

  alias Opentelemetry.Proto.Logs.V1.SeverityNumber

  @behaviour Logflare.Logs.Processor

  def handle_batch(resource_logs, _source) when is_list(resource_logs) do
    resource_logs
    |> Enum.map(&handle_resource_logs/1)
    |> List.flatten()
  end

  defp handle_resource_logs(%ResourceLogs{resource: resource, scope_logs: scope_logs}) do
    resource = Otel.handle_resource(resource)
    Enum.map(scope_logs, &handle_scope_logs(&1, resource))
  end

  defp handle_scope_logs(%ScopeLogs{scope: scope, log_records: log_records}, resource) do
    scope = Otel.handle_scope(scope)

    base_event = %{
      "metadata" => %{"type" => "otel_log"},
      "scope" => scope,
      "resource" => resource
    }

    Enum.map(log_records, &handle_log_record(&1, base_event))
  end

  defp handle_log_record(%LogRecord{} = log_record, base_event) do
    Map.merge(base_event, %{
      "event_message" => event_message(log_record),
      "body" => Otel.extract_value(log_record.body),
      "attributes" => Otel.handle_attributes(log_record.attributes),
      "severity_number" => SeverityNumber.value(log_record.severity_number),
      "severity_text" => log_record.severity_text,
      "trace_id" => Base.encode16(log_record.trace_id, case: :lower),
      "span_id" => Base.encode16(log_record.span_id, case: :lower),
      "timestamp" => log_record.time_unix_nano
    })
  end

  defp event_message(%{body: %{value: {:string_value, body}}}) do
    body
  end

  defp event_message(%{event_name: event_name}) do
    event_name
  end
end
