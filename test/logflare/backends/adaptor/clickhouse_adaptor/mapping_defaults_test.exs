defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaultsTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.Mapper

  setup_all do
    {:ok,
     log: Mapper.compile!(MappingDefaults.for_log()),
     metric: Mapper.compile!(MappingDefaults.for_metric()),
     trace: Mapper.compile!(MappingDefaults.for_trace()),
     simple_log: Mapper.compile!(MappingDefaults.for_log_simple()),
     simple_metric: Mapper.compile!(MappingDefaults.for_metric_simple()),
     simple_trace: Mapper.compile!(MappingDefaults.for_trace_simple())}
  end

  describe "for_type/1" do
    test "returns a MappingConfig for each log type" do
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type(:log)
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type(:metric)
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type(:trace)
    end

    test "raises for unknown log type" do
      assert_raise FunctionClauseError, fn ->
        apply(MappingDefaults, :for_type, [:unknown])
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

    test "resolves histogram bucket_counts and explicit_bounds", %{metric: compiled} do
      payload = %{
        "bucket_counts" => [1, 5, 10, 3, 0],
        "explicit_bounds" => [0.0, 5.0, 10.0, 25.0]
      }

      result = Mapper.map(payload, compiled)

      assert result["bucket_counts"] == [1, 5, 10, 3, 0]
      assert result["explicit_bounds"] == [0.0, 5.0, 10.0, 25.0]
    end

    test "resolves exponential histogram bucket counts from nested paths", %{metric: compiled} do
      payload = %{
        "exponential_histogram" => %{
          "positive" => %{"bucket_counts" => [2, 4, 8]},
          "negative" => %{"bucket_counts" => [1, 3, 5]}
        }
      }

      result = Mapper.map(payload, compiled)

      assert result["positive_bucket_counts"] == [2, 4, 8]
      assert result["negative_bucket_counts"] == [1, 3, 5]
    end

    test "resolves summary quantile fields", %{metric: compiled} do
      payload = %{
        "summary" => %{
          "quantile_values" => [1.5, 2.5, 9.9],
          "quantiles" => [0.5, 0.9, 0.99]
        }
      }

      result = Mapper.map(payload, compiled)

      assert result["quantile_values"] == [1.5, 2.5, 9.9]
      assert result["quantiles"] == [0.5, 0.9, 0.99]
    end

    test "decomposes exemplars into parallel arrays", %{metric: compiled} do
      payload = %{
        "exemplars" => [
          %{
            "filtered_attributes" => %{"key" => "val1"},
            "time_unix_nano" => 1_700_000_000_000_000_000,
            "value" => 42.5,
            "span_id" => "span-1",
            "trace_id" => "trace-1"
          },
          %{
            "filtered_attributes" => %{"key" => "val2"},
            "time_unix_nano" => 1_700_000_001_000_000_000,
            "value" => 99.0,
            "span_id" => "span-2",
            "trace_id" => "trace-2"
          }
        ]
      }

      result = Mapper.map(payload, compiled)

      assert result["exemplars.filtered_attributes"] == [
               %{"key" => "val1"},
               %{"key" => "val2"}
             ]

      assert result["exemplars.time_unix"] == [
               1_700_000_000_000_000_000,
               1_700_000_001_000_000_000
             ]

      assert result["exemplars.value"] == [42.5, 99.0]
      assert result["exemplars.span_id"] == ["span-1", "span-2"]
      assert result["exemplars.trace_id"] == ["trace-1", "trace-2"]
    end

    test "defaults array fields to empty lists when missing", %{metric: compiled} do
      result = Mapper.map(%{}, compiled)

      assert result["bucket_counts"] == []
      assert result["explicit_bounds"] == []
      assert result["positive_bucket_counts"] == []
      assert result["negative_bucket_counts"] == []
      assert result["quantile_values"] == []
      assert result["quantiles"] == []
      assert result["exemplars.filtered_attributes"] == []
      assert result["exemplars.time_unix"] == []
      assert result["exemplars.value"] == []
      assert result["exemplars.span_id"] == []
      assert result["exemplars.trace_id"] == []
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

    test "decomposes span events into parallel arrays", %{trace: compiled} do
      payload = %{
        "events" => [
          %{
            "time_unix_nano" => 1_700_000_000_000_000_000,
            "name" => "exception",
            "attributes" => %{"exception.message" => "not found"}
          },
          %{
            "time_unix_nano" => 1_700_000_001_000_000_000,
            "name" => "log",
            "attributes" => %{"log.message" => "retrying"}
          }
        ]
      }

      result = Mapper.map(payload, compiled)

      assert result["events.timestamp"] == [
               1_700_000_000_000_000_000,
               1_700_000_001_000_000_000
             ]

      assert result["events.name"] == ["exception", "log"]

      assert result["events.attributes"] == [
               %{"exception.message" => "not found"},
               %{"log.message" => "retrying"}
             ]
    end

    test "decomposes span links into parallel arrays", %{trace: compiled} do
      payload = %{
        "links" => [
          %{
            "trace_id" => "linked-trace-1",
            "span_id" => "linked-span-1",
            "trace_state" => "state1",
            "attributes" => %{"link.type" => "parent"}
          },
          %{
            "trace_id" => "linked-trace-2",
            "span_id" => "linked-span-2",
            "trace_state" => "state2",
            "attributes" => %{"link.type" => "child"}
          }
        ]
      }

      result = Mapper.map(payload, compiled)

      assert result["links.trace_id"] == ["linked-trace-1", "linked-trace-2"]
      assert result["links.span_id"] == ["linked-span-1", "linked-span-2"]
      assert result["links.trace_state"] == ["state1", "state2"]

      assert result["links.attributes"] == [
               %{"link.type" => "parent"},
               %{"link.type" => "child"}
             ]
    end

    test "defaults trace array fields to empty lists when missing", %{trace: compiled} do
      result = Mapper.map(%{}, compiled)

      assert result["events.timestamp"] == []
      assert result["events.name"] == []
      assert result["events.attributes"] == []
      assert result["links.trace_id"] == []
      assert result["links.span_id"] == []
      assert result["links.trace_state"] == []
      assert result["links.attributes"] == []
    end
  end

  describe "for_type_simple/1" do
    test "returns a MappingConfig for each type" do
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type_simple(:log)
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type_simple(:metric)
      assert %Mapper.MappingConfig{} = MappingDefaults.for_type_simple(:trace)
    end

    test "config_id_simple returns unique IDs" do
      assert MappingDefaults.config_id_simple(:log) != MappingDefaults.config_id(:log)
      assert MappingDefaults.config_id_simple(:metric) != MappingDefaults.config_id(:metric)
      assert MappingDefaults.config_id_simple(:trace) != MappingDefaults.config_id(:trace)
    end
  end

  describe "simple logs mapping" do
    test "produces flat string-keyed maps for attribute fields", %{simple_log: compiled} do
      payload = %{
        "event_message" => "Something happened",
        "project" => "my-project",
        "resource" => %{"service" => %{"name" => "my-svc"}},
        "scope" => %{"name" => "my-scope", "attributes" => %{"lib" => "otel"}},
        "metadata" => %{"level" => "error", "request_id" => "req-1"},
        "extra_field" => "kept",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      # Scalar fields should work identically
      assert result["event_message"] == "Something happened"
      assert result["project"] == "my-project"
      assert result["severity_text"] == "ERROR"
      assert result["severity_number"] == 17

      # resource_attributes should be flat %{String => String} via pick
      res_attrs = result["resource_attributes"]
      assert is_map(res_attrs)
      assert res_attrs["project"] == "my-project"
      assert res_attrs["service_name"] == "my-svc"

      # All values should be strings
      for {_k, v} <- res_attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      # scope_attributes should be flat
      scope_attrs = result["scope_attributes"]
      assert is_map(scope_attrs)

      for {_k, v} <- scope_attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      # log_attributes should be flat with exclude/elevate applied
      log_attrs = result["log_attributes"]
      assert is_map(log_attrs)
      refute Map.has_key?(log_attrs, "id")
      refute Map.has_key?(log_attrs, "event_message")
      refute Map.has_key?(log_attrs, "timestamp")
      # Elevated from metadata
      assert log_attrs["level"] == "error"
      assert log_attrs["request_id"] == "req-1"
      assert log_attrs["extra_field"] == "kept"

      for {_k, v} <- log_attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end
    end

    test "non-attribute fields are unchanged", %{simple_log: compiled} do
      result = Mapper.map(%{}, compiled)

      assert result["project"] == ""
      assert result["trace_id"] == ""
      assert result["trace_flags"] == 0
      assert result["severity_text"] == "INFO"
      assert result["severity_number"] == 9
    end
  end

  describe "simple metrics mapping" do
    test "produces flat attribute maps", %{simple_metric: compiled} do
      payload = %{
        "metric_name" => "http_requests_total",
        "value" => 42.5,
        "project" => "proj",
        "scope" => %{"name" => "my-scope", "attributes" => %{"lib" => "otel"}},
        "metadata" => %{"region" => "us-east-1", "level" => "info"},
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["metric_name"] == "http_requests_total"
      assert result["value"] == 42.5

      # attributes should be flat
      attrs = result["attributes"]
      assert is_map(attrs)

      for {_k, v} <- attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      # resource_attributes should be flat
      res_attrs = result["resource_attributes"]
      assert is_map(res_attrs)

      for {_k, v} <- res_attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      # scope_attributes should be flat
      scope_attrs = result["scope_attributes"]
      assert is_map(scope_attrs)

      for {_k, v} <- scope_attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end
    end

    test "exemplars.filtered_attributes becomes array of flat maps", %{simple_metric: compiled} do
      payload = %{
        "exemplars" => [
          %{
            "filtered_attributes" => %{"key" => "val1", "nested" => %{"a" => 1}},
            "time_unix_nano" => 1_700_000_000_000_000_000,
            "value" => 42.5,
            "span_id" => "span-1",
            "trace_id" => "trace-1"
          }
        ]
      }

      result = Mapper.map(payload, compiled)

      [filtered_attrs] = result["exemplars.filtered_attributes"]
      assert is_map(filtered_attrs)
      assert filtered_attrs["key"] == "val1"
      assert filtered_attrs["nested.a"] == "1"
    end
  end

  describe "simple traces mapping" do
    test "produces flat attribute maps", %{simple_trace: compiled} do
      payload = %{
        "trace_id" => "trace-abc",
        "span_id" => "span-def",
        "span_name" => "GET /api",
        "metadata" => %{"request_id" => "req-1"},
        "http.method" => "GET",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["trace_id"] == "trace-abc"
      assert result["span_name"] == "GET /api"

      # span_attributes should be flat
      span_attrs = result["span_attributes"]
      assert is_map(span_attrs)

      for {_k, v} <- span_attrs do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end
    end

    test "events.attributes becomes array of flat maps", %{simple_trace: compiled} do
      payload = %{
        "events" => [
          %{
            "time_unix_nano" => 1_700_000_000_000_000_000,
            "name" => "exception",
            "attributes" => %{"exception.message" => "not found", "nested" => %{"a" => 1}}
          }
        ]
      }

      result = Mapper.map(payload, compiled)

      [event_attrs] = result["events.attributes"]
      assert is_map(event_attrs)
      assert event_attrs["exception.message"] == "not found"
      assert event_attrs["nested.a"] == "1"
    end

    test "links.attributes becomes array of flat maps", %{simple_trace: compiled} do
      payload = %{
        "links" => [
          %{
            "trace_id" => "linked-trace-1",
            "span_id" => "linked-span-1",
            "trace_state" => "state1",
            "attributes" => %{"link.type" => "parent"}
          }
        ]
      }

      result = Mapper.map(payload, compiled)

      [link_attrs] = result["links.attributes"]
      assert is_map(link_attrs)
      assert link_attrs["link.type"] == "parent"
    end
  end
end
