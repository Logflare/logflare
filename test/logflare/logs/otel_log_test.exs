defmodule Logflare.Logs.OtelLogTest do
  use Logflare.DataCase
  alias Logflare.Logs.OtelLog
  alias Logflare.Logs.Otel

  alias Opentelemetry.Proto.Logs.V1.SeverityNumber

  describe "handle_batch/2" do
    setup do
      user = build(:user)
      source = insert(:source, user: user)
      resource_logs = Logflare.TestUtilsGrpc.random_resource_logs()
      %{resource_logs: resource_logs, source: source}
    end

    test "resource and scope attributes", %{
      resource_logs: resource_logs,
      source: source
    } do
      [%{"resource" => resource, "scope" => scope} | _] =
        OtelLog.handle_batch(resource_logs, source)

      assert is_map(resource)
      assert %{"name" => name, "version" => version, "attributes" => %{}} = scope
      assert is_binary(name)
      assert is_binary(version)
    end

    test "Creates params from a list with one ResourceLogs that contains a LogRecord", %{
      resource_logs: resource_logs,
      source: source
    } do
      [first_event | _rest] = OtelLog.handle_batch(resource_logs, source)
      %{scope_logs: [%{log_records: [first_record | _]} | _]} = List.first(resource_logs)

      expected_body = Otel.extract_value(first_record.body)
      expected_severity_number = SeverityNumber.value(first_record.severity_number)
      expected_severity_text = first_record.severity_text
      expected_trace_id = Base.encode16(first_record.trace_id, case: :lower)
      expected_span_id = Base.encode16(first_record.span_id, case: :lower)

      assert %{
               "event_message" => ^expected_body,
               "body" => ^expected_body,
               "severity_number" => ^expected_severity_number,
               "severity_text" => ^expected_severity_text,
               "trace_id" => ^expected_trace_id,
               "span_id" => ^expected_span_id
             } = first_event

      assert first_event["timestamp"] == first_record.time_unix_nano
    end

    test "json encodable log event body", %{
      resource_logs: resource_logs,
      source: source
    } do
      [params | _] = OtelLog.handle_batch(resource_logs, source)
      assert {:ok, _} = Jason.encode(params)
    end

    test "timestamps are unix milliseconds", %{
      resource_logs: resource_logs,
      source: source
    } do
      [params | _] = OtelLog.handle_batch(resource_logs, source)

      assert is_integer(params["timestamp"])
      assert Integer.digits(params["timestamp"]) |> length() == 19
    end
  end
end
