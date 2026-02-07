defmodule Logflare.LogEvent.TypeDetectionTest do
  use ExUnit.Case, async: true

  alias Logflare.LogEvent.TypeDetection

  describe "detect/1 with metadata.type (OTEL processor fast path)" do
    test "span -> :trace" do
      assert TypeDetection.detect(%{"metadata" => %{"type" => "span"}}) == :trace
    end

    test "metric -> :metric" do
      assert TypeDetection.detect(%{"metadata" => %{"type" => "metric"}}) == :metric
    end

    test "event -> :log" do
      assert TypeDetection.detect(%{"metadata" => %{"type" => "event"}}) == :log
    end

    test "otel_log -> :log" do
      assert TypeDetection.detect(%{"metadata" => %{"type" => "otel_log"}}) == :log
    end

    test "unknown metadata.type falls through to heuristics" do
      assert TypeDetection.detect(%{"metadata" => %{"type" => "unknown"}}) == :log
    end
  end

  describe "detect/1 trace heuristic" do
    test "trace_id + span_id + start_time" do
      params = %{
        "trace_id" => "abc123",
        "span_id" => "def456",
        "start_time" => "2025-12-22T20:04:25.369201Z"
      }

      assert TypeDetection.detect(params) == :trace
    end

    test "traceId + spanId + end_time (camelCase variants)" do
      params = %{
        "traceId" => "abc123",
        "spanId" => "def456",
        "end_time" => "2025-12-22T20:04:25.511409Z"
      }

      assert TypeDetection.detect(params) == :trace
    end

    test "otel_trace_id + otel_span_id + parent_span_id" do
      params = %{
        "otel_trace_id" => "abc123",
        "otel_span_id" => "def456",
        "parent_span_id" => "789ghi"
      }

      assert TypeDetection.detect(params) == :trace
    end

    test "parentSpanId (camelCase) satisfies span field requirement" do
      params = %{
        "trace_id" => "abc123",
        "span_id" => "def456",
        "parentSpanId" => "789ghi"
      }

      assert TypeDetection.detect(params) == :trace
    end

    test "duration satisfies span field requirement" do
      params = %{"trace_id" => "abc123", "span_id" => "def456", "duration" => "142ms"}
      assert TypeDetection.detect(params) == :trace
    end

    test "duration_ms satisfies span field requirement" do
      params = %{"trace_id" => "abc123", "span_id" => "def456", "duration_ms" => "142"}
      assert TypeDetection.detect(params) == :trace
    end

    test "duration_ns satisfies span field requirement" do
      params = %{"trace_id" => "abc123", "span_id" => "def456", "duration_ns" => "142000000"}
      assert TypeDetection.detect(params) == :trace
    end

    test "missing span_id -> :log" do
      params = %{
        "trace_id" => "abc123",
        "start_time" => "2025-12-22T20:04:25.369201Z"
      }

      assert TypeDetection.detect(params) == :log
    end

    test "missing trace_id -> :log" do
      params = %{
        "span_id" => "def456",
        "start_time" => "2025-12-22T20:04:25.369201Z"
      }

      assert TypeDetection.detect(params) == :log
    end

    test "trace_id + span_id without span-specific field -> :log" do
      params = %{"trace_id" => "abc123", "span_id" => "def456"}
      assert TypeDetection.detect(params) == :log
    end

    test "empty string trace_id -> :log" do
      params = %{
        "trace_id" => "",
        "span_id" => "def456",
        "start_time" => "2025-12-22T20:04:25.369201Z"
      }

      assert TypeDetection.detect(params) == :log
    end

    test "empty string span_id -> :log" do
      params = %{
        "trace_id" => "abc123",
        "span_id" => "",
        "start_time" => "2025-12-22T20:04:25.369201Z"
      }

      assert TypeDetection.detect(params) == :log
    end
  end

  describe "detect/1 metric heuristic" do
    test "metric_type + value" do
      params = %{"metric_type" => "sum", "value" => 25_607_364}
      assert TypeDetection.detect(params) == :metric
    end

    test "metric_type + gauge" do
      params = %{"metric_type" => "gauge", "gauge" => 42.5}
      assert TypeDetection.detect(params) == :metric
    end

    test "metric_type + count" do
      params = %{"metric_type" => "histogram", "count" => 1509}
      assert TypeDetection.detect(params) == :metric
    end

    test "metric_type + sum" do
      params = %{"metric_type" => "histogram", "sum" => 1000.0}
      assert TypeDetection.detect(params) == :metric
    end

    test "metric (name) + value" do
      params = %{"metric" => "http.request.duration", "value" => 142}
      assert TypeDetection.detect(params) == :metric
    end

    test "metric_type without value -> :log" do
      params = %{"metric_type" => "sum"}
      assert TypeDetection.detect(params) == :log
    end

    test "value without metric_type -> :log" do
      params = %{"value" => 42}
      assert TypeDetection.detect(params) == :log
    end

    test "empty string metric_type -> :log" do
      params = %{"metric_type" => "", "value" => 42}
      assert TypeDetection.detect(params) == :log
    end
  end

  describe "detect/1 log (default fallback)" do
    test "plain log message" do
      params = %{"event_message" => "Hello world"}
      assert TypeDetection.detect(params) == :log
    end

    test "empty map" do
      assert TypeDetection.detect(%{}) == :log
    end

    test "log with metadata but no type indicator" do
      params = %{
        "event_message" => "Sent 202 in 23ms",
        "metadata" => %{
          "level" => "info",
          "otel_trace_id" => "abc123",
          "otel_span_id" => "def456"
        }
      }

      assert TypeDetection.detect(params) == :log
    end

    test "log with nested metadata.req.traceId" do
      params = %{
        "event_message" => "GET /storage/v1/object",
        "metadata" => %{
          "req" => %{"traceId" => "8ff10a03a3b2144e-SJC"}
        }
      }

      assert TypeDetection.detect(params) == :log
    end
  end
end
