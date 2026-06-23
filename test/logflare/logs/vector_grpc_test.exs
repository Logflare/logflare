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
      assert event["value"] == "hello world"
      assert event["fields"]["host"] == "node-1"
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
      assert event["value"] == 42
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
      assert event["fields"]["nested"] == %{"k" => 1}
      assert event["fields"]["list"] == [true, 2.5]
    end
  end

  describe "handle_batch/2 for metric events" do
    test "converts a counter metric" do
      metric = %Event.EventWrapper{
        event:
          {:metric,
           %Event.Metric{
             name: "requests",
             namespace: "logflare",
             kind: :Incremental,
             timestamp: %Google.Protobuf.Timestamp{seconds: 1, nanos: 500},
             tags_v1: %{"env" => "test"},
             value: {:counter, %Event.Counter{value: 7.0}}
           }}
      }

      assert [event] = VectorGrpc.handle_batch([metric], :source)
      assert event["metadata"] == %{"type" => "vector_metric"}
      assert event["event_message"] == "logflare.requests"
      assert event["kind"] == "incremental"
      assert event["tags"] == %{"env" => "test"}
      assert event["value"] == %{"counter" => 7.0}
      assert event["timestamp"] == 1_000_000_500
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
      assert event["kind"] == "absolute"
      assert event["value"] == %{"gauge" => 0.42}
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

  test "handles empty event wrapper safely" do
    assert [event] = VectorGrpc.handle_batch([%Event.EventWrapper{event: nil}], :source)
    assert event["metadata"] == %{"type" => "vector_log"}
  end
end
