defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaultsTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.LogEvent.TypeDetection
  alias Logflare.Mapper
  alias Logflare.Mapper.MappingConfig.OutputFormat

  setup_all do
    {:ok,
     log: compile_map_output(:log),
     metric: compile_map_output(:metric),
     trace: compile_map_output(:trace)}
  end

  describe "for_type/1" do
    test "returns a MappingConfig with the matching RowBinary output" do
      for event_type <- [:log, :metric, :trace] do
        assert %Mapper.MappingConfig{
                 output: %OutputFormat{
                   format: :clickhouse_row_binary,
                   row_type: ^event_type
                 }
               } = MappingDefaults.for_type(event_type)
      end
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
        "project" => "abcdefghijklmnopqrst",
        "trace_id" => "abc123",
        "span_id" => "def456",
        "metadata" => %{"level" => "error"},
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["event_message"] == "Something happened"
      assert result["project"] == "abcdefghijklmnopqrst"
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

    test "resolves host from a resource-scoped key", %{log: compiled} do
      payload = %{"resource" => %{"host" => "ip-10-0-1-129.internal"}}

      assert Mapper.map(payload, compiled)["resource_attributes"]["host"] ==
               "ip-10-0-1-129.internal"
    end

    test "prefers metadata host over a resource-scoped host", %{log: compiled} do
      payload = %{
        "metadata" => %{"host" => "from-metadata"},
        "resource" => %{"host" => "from-resource"}
      }

      assert Mapper.map(payload, compiled)["resource_attributes"]["host"] == "from-metadata"
    end
  end

  describe "attribute maps elevate both metadata and attributes" do
    test "children of attributes land unprefixed", %{log: log, metric: metric, trace: trace} do
      payload = %{
        "attributes" => %{"_flow_name" => "branch-creation", "busy_ns" => 705_777_024_470},
        "metadata" => %{"type" => "span"},
        "timestamp" => 1_775_591_051_937_363
      }

      for {compiled, field} <- [
            {log, "log_attributes"},
            {metric, "attributes"},
            {trace, "span_attributes"}
          ] do
        attrs = Mapper.map(payload, compiled)[field]

        assert attrs["_flow_name"] == "branch-creation"
        assert attrs["busy_ns"] == "705777024470"
        assert attrs["type"] == "span"
        refute Map.has_key?(attrs, "attributes._flow_name")
        refute Map.has_key?(attrs, "attributes")
      end
    end

    test "metadata wins over attributes on a duplicate child key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "attributes" => %{"shared" => "from-attributes", "only_attrs" => "a"},
        "metadata" => %{"shared" => "from-metadata", "only_meta" => "m"},
        "timestamp" => 1_775_591_051_937_363
      }

      for {compiled, field} <- [
            {log, "log_attributes"},
            {metric, "attributes"},
            {trace, "span_attributes"}
          ] do
        attrs = Mapper.map(payload, compiled)[field]

        assert attrs["shared"] == "from-metadata"
        assert attrs["only_attrs"] == "a"
        assert attrs["only_meta"] == "m"
      end
    end

    test "a non-map attributes value is preserved as a literal key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "attributes" => "not-a-map",
        "metadata" => %{"type" => "span"},
        "timestamp" => 1_775_591_051_937_363
      }

      for {compiled, field} <- [
            {log, "log_attributes"},
            {metric, "attributes"},
            {trace, "span_attributes"}
          ] do
        attrs = Mapper.map(payload, compiled)[field]

        assert attrs["attributes"] == "not-a-map"
        assert attrs["type"] == "span"
      end
    end
  end

  describe "service_name resolution across all event types" do
    test "resolves an underscore-namespaced resource key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{"resource" => %{"_service_name" => "supadev", "environment" => "staging"}}

      for compiled <- [log, metric, trace] do
        result = Mapper.map(payload, compiled)

        assert result["service_name"] == "supadev"
        assert result["resource_attributes"]["service_name"] == "supadev"
      end
    end

    test "prefers the OTEL-standard path over the underscore form", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "resource" => %{"service" => %{"name" => "standard"}, "_service_name" => "underscore"}
      }

      for compiled <- [log, metric, trace] do
        result = Mapper.map(payload, compiled)

        assert result["service_name"] == "standard"
        assert result["resource_attributes"]["service_name"] == "standard"
      end
    end

    test "environment resolves from a resource-scoped key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{"resource" => %{"environment" => "staging"}}

      for compiled <- [log, metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["environment"] == "staging"
      end
    end

    test "environment prefers metadata over a resource-scoped key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "metadata" => %{"environment" => "prod"},
        "resource" => %{"environment" => "staging"}
      }

      for compiled <- [log, metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["environment"] == "prod"
      end
    end

    test "cluster and node resolve from top-level and resource-scoped keys", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      top_level = %{"cluster" => "clus-top", "node" => "node-top"}
      resource_scoped = %{"resource" => %{"cluster" => "clus-res", "node" => "node-res"}}
      metadata_context = %{"metadata" => %{"context" => %{"cluster" => "clus-ctx"}}}

      for compiled <- [log, metric, trace] do
        top = Mapper.map(top_level, compiled)["resource_attributes"]
        assert top["cluster"] == "clus-top"
        assert top["node"] == "node-top"

        res = Mapper.map(resource_scoped, compiled)["resource_attributes"]
        assert res["cluster"] == "clus-res"
        assert res["node"] == "node-res"

        ctx = Mapper.map(metadata_context, compiled)["resource_attributes"]
        assert ctx["cluster"] == "clus-ctx"
      end
    end

    test "cluster and node prefer metadata over top-level and resource keys", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "cluster" => "clus-top",
        "node" => "node-top",
        "metadata" => %{
          "cluster" => "clus-meta",
          "node" => "node-meta",
          "context" => %{"vm" => %{"node" => "node-vm"}}
        },
        "resource" => %{"cluster" => "clus-res", "node" => "node-res"}
      }

      for compiled <- [log, metric, trace] do
        res_attrs = Mapper.map(payload, compiled)["resource_attributes"]

        assert res_attrs["cluster"] == "clus-meta"
        assert res_attrs["node"] == "node-meta"
      end
    end

    test "region resolves a resource-scoped key", %{log: log, metric: metric, trace: trace} do
      payload = %{"resource" => %{"region" => "ap-southeast-2"}}

      for compiled <- [log, metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["region"] == "ap-southeast-2"
      end
    end

    test "region prefers the generic resource key over the underscore-namespaced form", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "resource" => %{"region" => "ap-southeast-2", "_project_region" => "eu-central-1"}
      }

      for compiled <- [log, metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["region"] == "ap-southeast-2"
      end
    end

    test "region resolves an underscore-namespaced resource key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{"resource" => %{"_project_region" => "eu-central-1"}}

      for compiled <- [log, metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["region"] == "eu-central-1"
      end
    end

    test "region prefers metadata over the underscore-namespaced resource key", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "metadata" => %{"region" => "us-east-1"},
        "resource" => %{"_project_region" => "eu-central-1"}
      }

      for compiled <- [log, metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["region"] == "us-east-1"
      end
    end

    test "falls back to journald identifiers", %{log: log, metric: metric, trace: trace} do
      for compiled <- [log, metric, trace] do
        assert Mapper.map(%{"SYSLOG_IDENTIFIER" => "postgres"}, compiled)["service_name"] ==
                 "postgres"

        assert Mapper.map(%{"_SYSTEMD_UNIT" => "pgbouncer.service"}, compiled)["service_name"] ==
                 "pgbouncer.service"
      end
    end
  end

  describe "resource_attributes pick entries are consistent across event types" do
    test "every type resolves the shared curated keys", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{
        "app_id" => "app-1",
        "machine_id" => "mach-1",
        "organization_id" => "org-1",
        "organization_slug" => "acme",
        "project" => "proj-1",
        "metadata" => %{
          "cluster" => "clus-1",
          "context" => %{"host" => "host-1", "vm" => %{"node" => "node-1"}},
          "environment" => "staging",
          "instance_id" => "inst-1",
          "region" => "us-east-1",
          "vector_file" => "/var/log/app.log",
          "vector_host" => "collector-1"
        }
      }

      shared_keys = ~w(
        application_id cluster environment host instance_id machine_id node
        organization_id organization_slug project region vector_file vector_host
      )

      for compiled <- [log, metric, trace] do
        res_attrs = Mapper.map(payload, compiled)["resource_attributes"]

        for key <- shared_keys do
          assert Map.has_key?(res_attrs, key), "missing #{key} in #{inspect(res_attrs)}"
        end

        assert res_attrs["application_id"] == "app-1"
        assert res_attrs["cluster"] == "clus-1"
        assert res_attrs["environment"] == "staging"
        assert res_attrs["host"] == "host-1"
        assert res_attrs["instance_id"] == "inst-1"
        assert res_attrs["machine_id"] == "mach-1"
        assert res_attrs["node"] == "node-1"
        assert res_attrs["organization_id"] == "org-1"
        assert res_attrs["organization_slug"] == "acme"
        assert res_attrs["project"] == "proj-1"
        assert res_attrs["region"] == "us-east-1"
        assert res_attrs["vector_file"] == "/var/log/app.log"
        assert res_attrs["vector_host"] == "collector-1"
      end
    end

    test "log uses application_name while metric and trace use application", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{"app_name" => "supadev"}

      assert Mapper.map(payload, log)["resource_attributes"]["application_name"] == "supadev"

      for compiled <- [metric, trace] do
        assert Mapper.map(payload, compiled)["resource_attributes"]["application"] == "supadev"
      end
    end

    test "project resolves from metadata.tenantId in every type", %{
      log: log,
      metric: metric,
      trace: trace
    } do
      payload = %{"metadata" => %{"tenantId" => "tenant-1"}}

      for compiled <- [log, metric, trace] do
        result = Mapper.map(payload, compiled)

        assert result["project"] == "tenant-1"
        assert result["resource_attributes"]["project"] == "tenant-1"
      end
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
        "project" => "abcdefghijklmnopqrst",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["metric_name"] == "http_requests_total"
      assert result["metric_description"] == "Total HTTP requests"
      assert result["metric_unit"] == "1"
      assert result["value"] == 42.5
      assert result["count"] == 100
      assert result["project"] == "abcdefghijklmnopqrst"
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
        "span_kind" => "Server",
        "duration" => 1500,
        "status" => %{"code" => "OK", "message" => "success"},
        "project" => "abcdefghijklmnopqrst",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      assert result["trace_id"] == "trace-abc"
      assert result["span_id"] == "span-def"
      assert result["parent_span_id"] == "span-parent"
      assert result["span_name"] == "GET /api/users"
      assert result["span_kind"] == "Server"
      assert result["duration"] == 1500
      assert result["status_code"] == "OK"
      assert result["status_message"] == "success"
    end

    test "normalizes span_kind to canonical low-cardinality values", %{trace: compiled} do
      mappings = [
        # Title-case form emitted by OtelTrace passes through unchanged
        {"Unspecified", "Unspecified"},
        {"Internal", "Internal"},
        {"Server", "Server"},
        {"Client", "Client"},
        {"Producer", "Producer"},
        {"Consumer", "Consumer"},
        # raw-JSON SPAN_KIND_* aliases normalize to the Title-case form
        {"SPAN_KIND_SERVER", "Server"},
        {"SPAN_KIND_CLIENT", "Client"},
        # case-insensitive
        {"client", "Client"}
      ]

      for {raw, expected} <- mappings do
        payload = %{
          "event_message" => "fetch POST api.logflare.app",
          "kind" => raw,
          "trace_id" => "trace-abc"
        }

        result = Mapper.map(payload, compiled)

        assert result["span_kind"] == expected
        assert result["span_name"] == "fetch POST api.logflare.app"
      end
    end

    test "falls back to Unspecified for blank or unmapped span_kind values", %{trace: compiled} do
      # unmapped value
      assert Mapper.map(%{"kind" => "bogus_kind", "trace_id" => "t1"}, compiled)["span_kind"] ==
               "Unspecified"

      # blank value
      assert Mapper.map(%{"kind" => "", "trace_id" => "t1"}, compiled)["span_kind"] ==
               "Unspecified"

      # missing value
      assert Mapper.map(%{"trace_id" => "t1"}, compiled)["span_kind"] == "Unspecified"
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

  describe "flat_map attribute values are strings" do
    test "log attribute values are all strings", %{log: compiled} do
      payload = %{
        "event_message" => "Something happened",
        "project" => "abcdefghijklmnopqrst",
        "resource" => %{"service" => %{"name" => "my-svc"}},
        "scope" => %{"name" => "my-scope", "attributes" => %{"lib" => "otel"}},
        "metadata" => %{"level" => "error", "request_id" => "req-1"},
        "extra_field" => "kept",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      for {_k, v} <- result["resource_attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      for {_k, v} <- result["scope_attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      for {_k, v} <- result["log_attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end
    end

    test "metric attribute values are all strings", %{metric: compiled} do
      payload = %{
        "metric_name" => "http_requests_total",
        "value" => 42.5,
        "project" => "proj",
        "scope" => %{"name" => "my-scope", "attributes" => %{"lib" => "otel"}},
        "metadata" => %{"region" => "us-east-1"},
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      for {_k, v} <- result["attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      for {_k, v} <- result["resource_attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end

      for {_k, v} <- result["scope_attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end
    end

    test "trace attribute values are all strings", %{trace: compiled} do
      payload = %{
        "trace_id" => "trace-abc",
        "span_id" => "span-def",
        "span_name" => "GET /api",
        "metadata" => %{"request_id" => "req-1"},
        "http.method" => "GET",
        "timestamp" => 1_700_000_000_000_000
      }

      result = Mapper.map(payload, compiled)

      for {_k, v} <- result["span_attributes"] do
        assert is_binary(v), "Expected string value, got: #{inspect(v)}"
      end
    end

    test "exemplars.filtered_attributes are flat maps with string values", %{metric: compiled} do
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

    test "events.attributes are flat maps with string values", %{trace: compiled} do
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

    test "links.attributes are flat maps with string values", %{trace: compiled} do
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

  @spec compile_map_output(TypeDetection.event_type()) :: reference()
  defp compile_map_output(event_type) do
    config = MappingDefaults.for_type(event_type)
    Mapper.compile!(%{config | output: nil})
  end
end
