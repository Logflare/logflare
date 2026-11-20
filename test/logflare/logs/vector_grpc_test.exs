defmodule Logflare.Logs.VectorGrpcTest do
  use ExUnit.Case, async: true

  alias Logflare.Logs.VectorGrpc

  describe "handle_batch/2 for log events" do
    test "extracts string value as event_message and fields" do
      log = %Event.EventWrapper{
        event:
          {:log,
           %Event.Log{
             value: %Event.Value{kind: {:raw_bytes, "hello world"}},
             fields: %{"host" => %Event.Value{kind: {:raw_bytes, "node-1"}}}
           }}
      }

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["metadata"] == %{"type" => "vector_log"}
      assert event["event_message"] == "hello world"
      assert event["host"] == "node-1"
    end

    test "falls back to fields['message'] when value is not a string" do
      log = %Event.EventWrapper{
        event:
          {:log,
           %Event.Log{
             value: %Event.Value{kind: {:integer, 42}},
             fields: %{"message" => %Event.Value{kind: {:raw_bytes, "field-msg"}}}
           }}
      }

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["event_message"] == "field-msg"
      assert event["message"] == "field-msg"
    end

    test "handles nested map and array values" do
      log = %Event.EventWrapper{
        event:
          {:log,
           %Event.Log{
             value: %Event.Value{kind: {:raw_bytes, "msg"}},
             fields: %{
               "nested" => %Event.Value{
                 kind:
                   {:map,
                    %Event.ValueMap{
                      fields: %{"k" => %Event.Value{kind: {:integer, 1}}}
                    }}
               },
               "list" => %Event.Value{
                 kind:
                   {:array,
                    %Event.ValueArray{
                      items: [
                        %Event.Value{kind: {:boolean, true}},
                        %Event.Value{kind: {:float, 2.5}}
                      ]
                    }}
               }
             }
           }}
      }

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["event_message"] == "msg"
      assert event["nested"] == %{"k" => 1}
      assert event["list"] == [true, 2.5]
    end
  end

  describe "handle_batch/2 for metric events" do
    test "converts a counter metric with unix-nanosecond timestamp" do
      seconds = 1_700_000_000
      nanos = 123

      metric = %Event.EventWrapper{
        event:
          {:metric,
           %Event.Metric{
             name: "requests",
             namespace: "logflare",
             kind: :Incremental,
             timestamp: %Google.Protobuf.Timestamp{seconds: seconds, nanos: nanos},
             tags_v1: %{"env" => "test"},
             value: {:counter, %Event.Counter{value: 7.0}}
           }}
      }

      assert [event] = VectorGrpc.handle_batch([metric], :source)
      assert event["metadata"] == %{"type" => "vector_metric"}
      assert event["event_message"] == "logflare.requests"
      assert event["metric_type"] == "sum"
      assert event["is_monotonic"] == true
      assert event["aggregation_temporality"] == "delta"
      assert event["attributes"] == %{"env" => "test"}
      assert event["value"] == 7.0
      # Matches the OTEL log ingestion format (nanoseconds since unix epoch).
      assert event["timestamp"] == seconds * 1_000_000_000 + nanos
    end

    test "converts a gauge metric without namespace" do
      metric = %Event.EventWrapper{
        event:
          {:metric,
           %Event.Metric{
             name: "cpu",
             namespace: "",
             kind: :Absolute,
             value: {:gauge, %Event.Gauge{value: 0.42}}
           }}
      }

      assert [event] = VectorGrpc.handle_batch([metric], :source)
      assert event["event_message"] == "cpu"
      assert event["metric_type"] == "gauge"
      assert event["aggregation_temporality"] == "cumulative"
      assert event["value"] == 0.42
    end
  end

  describe "handle_batch/2 for trace events" do
    test "extracts fields and uses message as event_message" do
      trace = %Event.EventWrapper{
        event:
          {:trace,
           %Event.Trace{
             fields: %{
               "message" => %Event.Value{kind: {:raw_bytes, "trace happened"}},
               "service" => %Event.Value{kind: {:raw_bytes, "api"}}
             }
           }}
      }

      assert [event] = VectorGrpc.handle_batch([trace], :source)
      assert event["metadata"] == %{"type" => "vector_trace"}
      assert event["event_message"] == "trace happened"
      assert event["fields"]["service"] == "api"
    end
  end

  describe "handle_batch/2 metric value variants" do
    test "set maps to a sum of its cardinality" do
      metric = build_metric({:set, %Event.Set{values: ["a", "b", "c"]}})

      assert [event] = VectorGrpc.handle_batch([metric], :source)
      assert event["metric_type"] == "sum"
      assert event["value"] == 3
    end

    test "aggregated histogram (v1) maps to OTEL histogram columns" do
      value =
        {:aggregated_histogram1,
         %Event.AggregatedHistogram1{buckets: [1.0, 5.0], counts: [2, 4], count: 6, sum: 21.0}}

      assert [event] = VectorGrpc.handle_batch([build_metric(value)], :source)
      assert event["metric_type"] == "histogram"
      assert event["count"] == 6
      assert event["sum"] == 21.0
      assert event["explicit_bounds"] == [1.0, 5.0]
      assert event["bucket_counts"] == [2, 4]
    end

    test "aggregated summary (v1) maps to OTEL summary columns" do
      value =
        {:aggregated_summary1,
         %Event.AggregatedSummary1{
           quantiles: [0.5, 0.9],
           values: [10.0, 20.0],
           count: 5,
           sum: 50.0
         }}

      assert [event] = VectorGrpc.handle_batch([build_metric(value)], :source)
      assert event["metric_type"] == "summary"
      assert event["count"] == 5
      assert event["sum"] == 50.0
      assert event["quantiles"] == [0.5, 0.9]
      assert event["quantile_values"] == [10.0, 20.0]
    end

    test "later aggregate variants fall back to family + count/sum" do
      value =
        {:aggregated_histogram2, %Event.AggregatedHistogram2{buckets: [], count: 7, sum: 12.5}}

      assert [event] = VectorGrpc.handle_batch([build_metric(value)], :source)
      assert event["metric_type"] == "histogram"
      assert event["count"] == 7
      assert event["sum"] == 12.5
    end

    test "distributions are not yet mapped to an OTEL type (open question)" do
      value =
        {:distribution1,
         %Event.Distribution1{values: [1.0], sample_rates: [1], statistic: :Histogram}}

      assert [event] = VectorGrpc.handle_batch([build_metric(value)], :source)
      assert event["metric_type"] == nil
      refute Map.has_key?(event, "value")
    end

    test "a metric with no value carries no metric_type or value" do
      metric = %Event.EventWrapper{
        event: {:metric, %Event.Metric{name: "m", kind: :Absolute}}
      }

      assert [event] = VectorGrpc.handle_batch([metric], :source)
      refute Map.has_key?(event, "metric_type")
      refute Map.has_key?(event, "value")
    end

    test "an unknown metric kind yields a nil aggregation_temporality" do
      metric = %Event.EventWrapper{
        event:
          {:metric,
           %Event.Metric{name: "m", kind: nil, value: {:gauge, %Event.Gauge{value: 1.0}}}}
      }

      assert [event] = VectorGrpc.handle_batch([metric], :source)
      assert event["aggregation_temporality"] == nil
    end
  end

  describe "handle_batch/2 value extraction" do
    test "non-utf8 raw_bytes are base64-encoded" do
      bytes = <<0xFF, 0xFE>>
      log = log_with_fields(%{"blob" => %Event.Value{kind: {:raw_bytes, bytes}}})

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["blob"] == Base.encode64(bytes)
    end

    test "timestamp values become unix nanoseconds" do
      ts = %Google.Protobuf.Timestamp{seconds: 1_700_000_000, nanos: 123}
      log = log_with_fields(%{"at" => %Event.Value{kind: {:timestamp, ts}}})

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["at"] == 1_700_000_000 * 1_000_000_000 + 123
    end

    test "null values become nil" do
      log = log_with_fields(%{"n" => %Event.Value{kind: {:null, :NULL_VALUE}}})

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert Map.fetch!(event, "n") == nil
    end
  end

  describe "handle_batch/2 metadata handling" do
    test "preserves a nested metadata object and only adds the type marker" do
      log = %Event.EventWrapper{
        event:
          {:log,
           %Event.Log{
             value: %Event.Value{
               kind:
                 {:map,
                  %Event.ValueMap{
                    fields: %{
                      "metadata" => %Event.Value{
                        kind:
                          {:map,
                           %Event.ValueMap{
                             fields: %{"level" => %Event.Value{kind: {:raw_bytes, "error"}}}
                           }}
                      }
                    }
                  }}
             }
           }}
      }

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["metadata"]["level"] == "error"
      assert event["metadata"]["type"] == "vector_log"
    end

    test "vector_metadata is extracted from metadata_full" do
      log = %Event.EventWrapper{
        event:
          {:log,
           %Event.Log{
             value: %Event.Value{kind: {:raw_bytes, "hi"}},
             metadata_full: %Event.Metadata{source_id: "src-1", source_type: "docker_logs"}
           }}
      }

      assert [event] = VectorGrpc.handle_batch([log], :source)

      assert event["vector_metadata"] == %{
               "value" => nil,
               "source_id" => "src-1",
               "source_type" => "docker_logs"
             }
    end
  end

  describe "log event_message resolution" do
    test "stringifies a scalar root value when no message field exists" do
      log = %Event.EventWrapper{
        event: {:log, %Event.Log{value: %Event.Value{kind: {:integer, 42}}}}
      }

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["event_message"] == "42"
    end

    test "falls back to an event_message field" do
      log =
        log_with_fields(
          %{"event_message" => %Event.Value{kind: {:raw_bytes, "from-field"}}},
          %Event.Value{kind: {:integer, 1}}
        )

      assert [event] = VectorGrpc.handle_batch([log], :source)
      assert event["event_message"] == "from-field"
    end
  end

  test "handles empty event wrapper safely" do
    assert [event] = VectorGrpc.handle_batch([%Event.EventWrapper{event: nil}], :source)
    assert event["metadata"] == %{"type" => "vector_log"}
  end

  defp build_metric(value, kind \\ :Incremental) do
    %Event.EventWrapper{
      event: {:metric, %Event.Metric{name: "m", namespace: "ns", kind: kind, value: value}}
    }
  end

  defp log_with_fields(fields, value \\ %Event.Value{kind: {:raw_bytes, "msg"}}) do
    %Event.EventWrapper{event: {:log, %Event.Log{value: value, fields: fields}}}
  end
end
