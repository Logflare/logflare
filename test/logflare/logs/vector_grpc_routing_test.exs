defmodule Logflare.Logs.VectorGrpcRoutingTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.LogEvent
  alias Logflare.Logs.VectorGrpc
  alias Logflare.Mapper
  alias Logflare.TestUtilsGrpc

  setup do
    user = insert(:user)
    source = insert(:source, user: user)
    backend = build(:backend, type: :clickhouse) |> Map.put(:token, Ecto.UUID.generate())

    {:ok, source: source, backend: backend}
  end

  describe "ClickHouse table routing for Vector events" do
    test "metric events classify as :metric and route to the otel_metrics table", %{
      source: source,
      backend: backend
    } do
      assert [params] =
               VectorGrpc.handle_batch([TestUtilsGrpc.random_vector_metric_event()], source)

      le = LogEvent.make(params, %{source: source})

      assert le.event_type == :metric

      table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, le.event_type)
      assert String.starts_with?(table, "otel_metrics_")
    end

    test "trace events classify as :trace and route to the otel_traces table", %{
      source: source,
      backend: backend
    } do
      assert [params] =
               VectorGrpc.handle_batch([TestUtilsGrpc.random_vector_trace_event()], source)

      le = LogEvent.make(params, %{source: source})

      assert le.event_type == :trace

      table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, le.event_type)
      assert String.starts_with?(table, "otel_traces_")
    end

    test "log events classify as :log and route to the otel_logs table", %{
      source: source,
      backend: backend
    } do
      assert [params] = VectorGrpc.handle_batch([TestUtilsGrpc.random_vector_log_event()], source)

      le = LogEvent.make(params, %{source: source})

      assert le.event_type == :log

      table = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, le.event_type)
      assert String.starts_with?(table, "otel_logs_")
    end
  end

  describe "ClickHouse field mapping for Vector metrics" do
    test "a counter maps to OTEL sum columns with a scalar value", %{source: source} do
      counter = %Event.EventWrapper{
        event:
          {:metric,
           %Event.Metric{
             name: "requests_total",
             namespace: "logflare",
             kind: :Incremental,
             tags_v1: %{"region" => "us-east-1"},
             value: {:counter, %Event.Counter{value: 42.0}}
           }}
      }

      assert [params] = VectorGrpc.handle_batch([counter], source)
      le = LogEvent.make(params, %{source: source})
      assert {:ok, mapped} = Mapper.run(le.body, MappingDefaults.for_metric())

      assert mapped["metric_name"] == "requests_total"
      # metric_type is an enum8 column: "sum" resolves to the code 2.
      assert mapped["metric_type"] == 2
      assert mapped["value"] == 42.0
      assert mapped["is_monotonic"] == true
      assert mapped["aggregation_temporality"] == "delta"
    end

    test "a gauge maps to OTEL gauge columns with a scalar value", %{source: source} do
      gauge = %Event.EventWrapper{
        event:
          {:metric,
           %Event.Metric{
             name: "cpu_seconds",
             kind: :Absolute,
             value: {:gauge, %Event.Gauge{value: 0.75}}
           }}
      }

      assert [params] = VectorGrpc.handle_batch([gauge], source)
      le = LogEvent.make(params, %{source: source})
      assert {:ok, mapped} = Mapper.run(le.body, MappingDefaults.for_metric())

      assert mapped["metric_name"] == "cpu_seconds"
      # metric_type is an enum8 column: "gauge" resolves to the code 1.
      assert mapped["metric_type"] == 1
      assert mapped["value"] == 0.75
    end
  end
end
