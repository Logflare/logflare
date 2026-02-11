defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaultsTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.Mapper

  setup_all do
    {:ok,
     log: Mapper.compile!(MappingDefaults.for_log()),
     metric: Mapper.compile!(MappingDefaults.for_metric()),
     trace: Mapper.compile!(MappingDefaults.for_trace())}
  end

  describe "for_type/1" do
    test "returns a MappingConfig for each log type" do
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type(:log)
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type(:metric)
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type(:trace)
    end

    test "raises for unknown log type" do
      assert_raise FunctionClauseError, fn ->
        MappingDefaults.for_type(:unknown)
      end
    end
  end

  describe "logs mapping" do
    test "resolves scalar fields from OTEL-style payload", %{log: compiled} do
      payload = %{
        "event_message" => "Something happened",
        "project" => "my-project",
        "trace_id" => "abc123",
        "span_id" => "def456",
        "metadata" => %{"level" => "error"},
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["event_message"] == "Something happened"
      assert result["project"] == "my-project"
      assert result["trace_id"] == "abc123"
      assert result["span_id"] == "def456"
      assert result["severity_text"] == "ERROR"
      assert result["severity_number"] == 17
      assert result["timestamp"] == 1_700_000_000_000_000_000
    end

    test "applies defaults for missing fields", %{log: compiled} do
      result = Mapper.map(%{"event_message" => "hello"}, compiled)

      assert result["project"] == ""
      assert result["trace_id"] == ""
      assert result["trace_flags"] == 0
      assert result["severity_text"] == "INFO"
      assert result["severity_number"] == 9
      assert result["service_name"] == ""
    end

    test "produces log_attributes with exclude_keys and elevate_keys", %{log: compiled} do
      payload = %{
        "id" => "should-be-excluded",
        "event_message" => "also excluded",
        "timestamp" => 123,
        "project" => "proj",
        "metadata" => %{"level" => "info", "request_id" => "req-1"},
        "extra_field" => "kept"
      }

      result = Mapper.map(payload, compiled)
      log_attrs = result["log_attributes"]

      refute Map.has_key?(log_attrs, "id")
      refute Map.has_key?(log_attrs, "event_message")
      refute Map.has_key?(log_attrs, "timestamp")
      assert log_attrs["extra_field"] == "kept"
      assert log_attrs["project"] == "proj"
      assert log_attrs["level"] == "info"
      assert log_attrs["request_id"] == "req-1"
    end

    test "builds resource_attributes via pick entries", %{log: compiled} do
      payload = %{
        "event_message" => "test",
        "project" => "proj-123",
        "service_name" => "my-svc",
        "metadata" => %{"region" => "us-east-1"}
      }

      result = Mapper.map(payload, compiled)
      res_attrs = result["resource_attributes"]

      assert res_attrs["project"] == "proj-123"
      assert res_attrs["service_name"] == "my-svc"
      assert res_attrs["region"] == "us-east-1"
    end

    test "coalesces severity_text from different paths", %{log: compiled} do
      result1 = Mapper.map(%{"level" => "warn"}, compiled)
      assert result1["severity_text"] == "WARN"

      result2 = Mapper.map(%{"severityText" => "debug"}, compiled)
      assert result2["severity_text"] == "DEBUG"
    end
  end

  describe "metrics mapping" do
    test "resolves metric fields", %{metric: compiled} do
      payload = %{
        "metric_name" => "http_requests_total",
        "metric_description" => "Total HTTP requests",
        "metric_unit" => "1",
        "value" => 42.5,
        "count" => 100,
        "project" => "my-proj",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["metric_name"] == "http_requests_total"
      assert result["metric_description"] == "Total HTTP requests"
      assert result["metric_unit"] == "1"
      assert result["value"] == 42.5
      assert result["count"] == 100
      assert result["project"] == "my-proj"
    end

    test "infers metric_type from structural cues", %{metric: compiled} do
      result_gauge = Mapper.map(%{"gauge" => %{"value" => 1.0}}, compiled)
      assert result_gauge["metric_type"] == 1

      result_sum = Mapper.map(%{"sum" => %{"value" => 5.0}}, compiled)
      assert result_sum["metric_type"] == 2

      result_hist = Mapper.map(%{"histogram" => %{"count" => 10}}, compiled)
      assert result_hist["metric_type"] == 3

      result_exp =
        Mapper.map(%{"exponential_histogram" => %{"count" => 10}}, compiled)

      assert result_exp["metric_type"] == 4

      result_summary = Mapper.map(%{"summary" => %{"count" => 5}}, compiled)
      assert result_summary["metric_type"] == 5
    end

    test "defaults numeric fields to zero", %{metric: compiled} do
      result = Mapper.map(%{}, compiled)

      assert result["value"] == 0
      assert result["count"] == 0
      assert result["sum"] == 0
      assert result["min"] == 0
      assert result["max"] == 0
      assert result["scale"] == 0
      assert result["zero_count"] == 0
      assert result["flags"] == 0
    end
  end

  describe "traces mapping" do
    test "resolves trace fields from OTEL-style payload", %{trace: compiled} do
      payload = %{
        "trace_id" => "trace-abc",
        "span_id" => "span-def",
        "parent_span_id" => "span-parent",
        "span_name" => "GET /api/users",
        "span_kind" => "server",
        "duration" => 1500,
        "status" => %{"code" => "OK", "message" => "success"},
        "project" => "my-proj",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["trace_id"] == "trace-abc"
      assert result["span_id"] == "span-def"
      assert result["parent_span_id"] == "span-parent"
      assert result["span_name"] == "GET /api/users"
      assert result["span_kind"] == "server"
      assert result["duration"] == 1500
      assert result["status_code"] == "OK"
      assert result["status_message"] == "success"
    end

    test "coalesces camelCase trace field names", %{trace: compiled} do
      payload = %{
        "traceId" => "trace-1",
        "spanId" => "span-1",
        "parentSpanId" => "parent-1",
        "traceState" => "some-state"
      }

      result = Mapper.map(payload, compiled)

      assert result["trace_id"] == "trace-1"
      assert result["span_id"] == "span-1"
      assert result["parent_span_id"] == "parent-1"
      assert result["trace_state"] == "some-state"
    end

    test "defaults string fields to empty string", %{trace: compiled} do
      result = Mapper.map(%{}, compiled)

      assert result["trace_id"] == ""
      assert result["span_id"] == ""
      assert result["parent_span_id"] == ""
      assert result["span_name"] == ""
      assert result["span_kind"] == ""
      assert result["status_code"] == ""
      assert result["status_message"] == ""
      assert result["service_name"] == ""
    end

    test "produces span_attributes with exclude_keys and elevate_keys", %{trace: compiled} do
      payload = %{
        "id" => "should-be-excluded",
        "event_message" => "also excluded",
        "timestamp" => 123,
        "trace_id" => "t1",
        "metadata" => %{"request_id" => "req-1"},
        "http.method" => "GET"
      }

      result = Mapper.map(payload, compiled)
      span_attrs = result["span_attributes"]

      refute Map.has_key?(span_attrs, "id")
      refute Map.has_key?(span_attrs, "event_message")
      refute Map.has_key?(span_attrs, "timestamp")
      assert span_attrs["trace_id"] == "t1"
      assert span_attrs["http.method"] == "GET"
      assert span_attrs["request_id"] == "req-1"
    end
  end
end
