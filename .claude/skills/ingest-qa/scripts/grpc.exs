defmodule IngestQA.Grpc do
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.LogsService.Stub
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Logs.V1.LogRecord
  alias Opentelemetry.Proto.Logs.V1.ResourceLogs
  alias Opentelemetry.Proto.Logs.V1.ScopeLogs
  alias Opentelemetry.Proto.Resource.V1.Resource

  def run(messages, opts) do
    {:ok, sup} = GRPC.Client.Supervisor.start_link([])
    Process.unlink(sup)

    headers = [{"x-api-key", opts[:api_key]}, {"x-source", opts[:source_token]}]
    {:ok, channel} = GRPC.Stub.connect("localhost:#{opts[:port]}", headers: headers)

    result =
      case Stub.export(channel, request(messages)) do
        {:ok, _} -> "ok"
        other -> "FAIL #{inspect(other)}"
      end

    IO.puts("grpc #{result}")
    Supervisor.stop(sup)
  end

  defp request(messages) do
    now = System.os_time(:nanosecond)

    records =
      for message <- messages do
        %LogRecord{
          time_unix_nano: now,
          observed_time_unix_nano: now,
          body: %AnyValue{value: {:string_value, message}}
        }
      end

    %ExportLogsServiceRequest{
      resource_logs: [
        %ResourceLogs{
          resource: %Resource{attributes: [string_attr("service.name", "ingest-qa")]},
          scope_logs: [
            %ScopeLogs{scope: %InstrumentationScope{name: "ingest-qa"}, log_records: records}
          ]
        }
      ]
    }
  end

  defp string_attr(key, value),
    do: %KeyValue{key: key, value: %AnyValue{value: {:string_value, value}}}
end

IngestQA.Grpc.run(String.split("{{MESSAGES}}", "|"),
  api_key: "{{API_KEY}}",
  source_token: "{{SOURCE_TOKEN}}",
  port: "{{GRPC_PORT}}"
)
