defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.VMTest do
  use Logflare.DataCase, async: false

  import Logflare.VictoriaMetricsTestHelpers

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor

  @moduletag :victoria_metrics
  @vm_retry [sleep: 250, duration: 30_000]

  setup :setup_vm

  test "connection check sends a valid empty remote write request", %{backend: backend} do
    assert :ok = VictoriaMetricsAdaptor.test_connection(backend)
  end

  test "gauge samples retain their value, labels and millisecond timestamp", %{source: source} do
    timestamp_ms = System.system_time(:millisecond) - 1_000

    event =
      metric_event(source,
        event_message: "http.server.duration",
        metric_type: "gauge",
        value: 42.5,
        timestamp: timestamp_ms * 1_000_000 + 456_789,
        attributes: %{"method" => "GET", "status" => "200"}
      )

    assert [series] = ingest([event], source)
    assert series["values"] == [42.5]
    assert series["timestamps"] == [timestamp_ms]

    assert series["metric"] == %{
             "__name__" => "http_server_duration",
             "source" => source.name,
             "method" => "GET",
             "status" => "200"
           }
  end

  test "cumulative sums support monotonic and non-monotonic values", %{source: source} do
    events =
      for {name, value, monotonic} <- [
            {"requests_total", 100, true},
            {"queue_change", -3, false}
          ] do
        metric_event(source,
          event_message: name,
          metric_type: "sum",
          aggregation_temporality: "cumulative",
          is_monotonic: monotonic,
          value: value
        )
      end

    series = ingest(events, source, 2)
    assert values_by_name(series) == %{"requests_total" => [100], "queue_change" => [-3]}
  end

  test "explicit histograms become cumulative buckets, count and sum", %{source: source} do
    event =
      metric_event(source,
        event_message: "latency",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 10,
        sum: 500.0,
        bucket_counts: [2, 5, 3],
        explicit_bounds: [100.0, 500.0],
        attributes: %{"method" => "GET"}
      )

    series = ingest([event], source, 5)
    assert buckets(series, "latency_bucket") == %{"100" => [2], "500" => [7], "+Inf" => [10]}
    assert values_by_name(series)["latency_count"] == [10]
    assert values_by_name(series)["latency_sum"] == [500]
    assert Enum.all?(series, &(&1["metric"]["method"] == "GET"))
  end

  test "exports every histogram observation and omits absent sums", %{source: source} do
    older = System.system_time(:millisecond) - 60_000
    newer = older + 1_000

    events =
      for {name, first_sum, second_sum} <- [{"latency", 1.7, 3.3}, {"no_sum", nil, nil}],
          {timestamp, counts, count, sum} <- [
            {newer, [4, 4, 2], 10, second_sum},
            {older, [2, 3, 1], 6, first_sum}
          ] do
        metric_event(source,
          event_message: name,
          metric_type: "histogram",
          aggregation_temporality: "cumulative",
          count: count,
          sum: sum,
          bucket_counts: counts,
          explicit_bounds: [0.1, 0.5],
          timestamp: timestamp * 1_000_000,
          resource: %{
            "service.name" => "checkout",
            "service.namespace" => "shop",
            "service.instance.id" => "checkout-1"
          },
          attributes: %{"http.route" => "/checkout"}
        )
      end

    expected =
      for name <- ["latency", "no_sum"],
          {suffix, extra_labels, values} <-
            [
              {"_count", %{}, [6, 10]},
              {"_bucket", %{"le" => "0.1"}, [2, 4]},
              {"_bucket", %{"le" => "0.5"}, [5, 8]},
              {"_bucket", %{"le" => "+Inf"}, [6, 10]}
            ] ++ if(name == "latency", do: [{"_sum", %{}, [1.7, 3.3]}], else: []),
          into: %{} do
        labels =
          Map.merge(
            %{
              "__name__" => name <> suffix,
              "source" => source.name,
              "job" => "shop/checkout",
              "instance" => "checkout-1",
              "_http_route" => "/checkout"
            },
            extra_labels
          )

        {labels, Enum.zip([older, newer], values)}
      end

    assert {:ok, 4} = Backends.ingest_logs(events, source)

    TestUtils.retry_assert(@vm_retry, fn ->
      actual =
        Map.new(export(source), fn series ->
          {series["metric"], Enum.zip(series["timestamps"], series["values"])}
        end)

      assert actual == expected
    end)
  end

  test "missing temporality remains cumulative for sums and histograms", %{source: source} do
    events = [
      metric_event(source, event_message: "legacy_total", metric_type: "sum", value: 12),
      metric_event(source,
        event_message: "legacy_latency",
        metric_type: "histogram",
        count: 3,
        sum: 2.5,
        bucket_counts: [1, 2],
        explicit_bounds: [1.0]
      )
    ]

    series = ingest(events, source, 5)
    assert values_by_name(series)["legacy_total"] == [12]
    assert values_by_name(series)["legacy_latency_count"] == [3]
    assert values_by_name(series)["legacy_latency_sum"] == [2.5]
    assert buckets(series, "legacy_latency_bucket") == %{"1" => [1], "+Inf" => [3]}
  end

  test "histogram bounds retain plain decimal labels", %{source: source} do
    event =
      metric_event(source,
        event_message: "latency",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 4,
        sum: 1.0,
        bucket_counts: [1, 1, 1, 1, 0],
        explicit_bounds: [1.0e-9, 0.005, 2.5, 1000.0]
      )

    assert ingest([event], source, 7) |> buckets("latency_bucket") == %{
             "0.000000001" => [1],
             "0.005" => [2],
             "2.5" => [3],
             "1000" => [4],
             "+Inf" => [4]
           }
  end

  test "resource service attributes become job and instance labels", %{source: source} do
    event =
      metric_event(source,
        metric_type: "gauge",
        value: 1,
        resource: %{
          "service.name" => "checkout",
          "service.namespace" => "shop",
          "service.instance.id" => "checkout-1"
        }
      )

    assert [series] = ingest([event], source)
    assert %{"job" => "shop/checkout", "instance" => "checkout-1"} = series["metric"]
  end

  test "otherwise identical metrics from different service instances remain separate", %{
    source: source
  } do
    timestamp_ns = System.system_time(:nanosecond)

    events =
      for {instance, value} <- [{"checkout-1", 10}, {"checkout-2", 20}] do
        metric_event(source,
          event_message: "queue_depth",
          metric_type: "gauge",
          value: value,
          timestamp: timestamp_ns,
          resource: %{"service.name" => "checkout", "service.instance.id" => instance}
        )
      end

    series = ingest(events, source, 2)
    assert Enum.all?(series, &(&1["metric"]["job"] == "checkout"))

    assert Map.new(series, &{&1["metric"]["instance"], &1["values"]}) == %{
             "checkout-1" => [10],
             "checkout-2" => [20]
           }
  end

  test "service name alone becomes job without an instance", %{source: source} do
    event =
      metric_event(source,
        metric_type: "gauge",
        value: 1,
        resource: %{"service.name" => "checkout"}
      )

    assert [series] = ingest([event], source)
    assert series["metric"]["job"] == "checkout"
    refute Map.has_key?(series["metric"], "instance")
  end

  test "reserved attribute labels survive under exported names", %{source: source} do
    event =
      metric_event(source,
        metric_type: "gauge",
        value: 1,
        resource: %{"service.name" => "checkout"},
        attributes: %{"source" => "client", "job" => "batch", "region" => "eu"}
      )

    assert [series] = ingest([event], source)

    assert %{
             "job" => "checkout",
             "exported_source" => "client",
             "exported_job" => "batch",
             "region" => "eu"
           } = series["metric"]

    assert series["metric"]["source"] == source.name
  end

  test "scalar attributes become strings while list and map values are skipped", %{source: source} do
    event =
      metric_event(source,
        metric_type: "gauge",
        value: 1,
        attributes: %{
          "queue" => "default",
          "retry" => true,
          "ratio" => 1000.0,
          "tags" => ["a", 0.3],
          "http" => %{"method" => "GET"}
        }
      )

    assert [series] = ingest([event], source)
    assert %{"queue" => "default", "retry" => "true", "ratio" => "1000"} = series["metric"]
    refute Map.has_key?(series["metric"], "tags")
    refute Map.has_key?(series["metric"], "http")
  end

  test "metric and normalized attribute names are accepted as Prometheus identifiers", %{
    source: source
  } do
    event =
      metric_event(source,
        event_message: "9lives.req/sec-é:x",
        metric_type: "gauge",
        value: 1,
        attributes: %{"http.route" => "/", "1st" => "a"}
      )

    assert [series] = ingest([event], source)

    assert %{
             "__name__" => "_9lives_req_sec___:x",
             "_http_route" => "/",
             "_1st" => "a"
           } = series["metric"]
  end

  test "unsupported metrics and non-metric events do not discard valid metrics", %{
    source: source,
    backend: backend
  } do
    events = [
      build(:log_event, source: source, event_message: "a_log"),
      %{build(:log_event, source: source, event_message: "a_trace") | event_type: :trace},
      metric_event(source,
        event_message: "exponential",
        metric_type: "exponential_histogram",
        count: 1
      ),
      metric_event(source, event_message: "summary", metric_type: "summary", value: 1),
      metric_event(source,
        event_message: "monotonic_delta",
        metric_type: "sum",
        is_monotonic: true,
        aggregation_temporality: "delta",
        value: 1
      ),
      metric_event(source,
        event_message: "non_monotonic_delta",
        metric_type: "sum",
        is_monotonic: false,
        aggregation_temporality: "delta",
        value: 3
      ),
      metric_event(source,
        event_message: "unspecified_sum",
        metric_type: "sum",
        aggregation_temporality: "unspecified",
        value: 1
      ),
      metric_event(source,
        event_message: "delta_histogram",
        metric_type: "histogram",
        aggregation_temporality: "delta",
        count: 1,
        bucket_counts: [1],
        explicit_bounds: []
      ),
      metric_event(source, event_message: "kept", metric_type: "gauge", value: 1)
    ]

    ingest(events, source)
    assert_drops(backend, %{not_a_metric: 2, unsupported_type: 2, non_cumulative: 4})
    assert [series] = export(source)
    assert series["metric"]["__name__"] == "kept"
    assert series["values"] == [1]
  end

  test "malformed data points do not discard a valid sample in the same batch", %{
    source: source,
    backend: backend
  } do
    malformed = [
      [event_message: 123, metric_type: "gauge", value: 1.0],
      [metric_type: "gauge"],
      [metric_type: "gauge", value: "12"],
      [metric_type: "histogram", count: 1, bucket_counts: ["1"]],
      [metric_type: "histogram", count: 2, bucket_counts: [1, 1], explicit_bounds: ["1"]],
      [metric_type: "histogram", count: 3, bucket_counts: [1, 1, 1], explicit_bounds: [2.0, 1.0]],
      [metric_type: "histogram", count: 3, bucket_counts: [1, 1, 1], explicit_bounds: [1.0, 1]],
      [metric_type: "histogram", count: 5, bucket_counts: [2, 3], explicit_bounds: [1.0, 2.0]],
      [metric_type: "histogram", count: -1]
    ]

    events =
      Enum.map(malformed, fn attrs ->
        metric_event(source, Keyword.put(attrs, :aggregation_temporality, "cumulative"))
      end) ++ [metric_event(source, event_message: "kept", metric_type: "gauge", value: 1)]

    ingest(events, source)
    assert_drops(backend, %{invalid: 9})
    assert [series] = export(source)
    assert series["metric"]["__name__"] == "kept"
    assert series["values"] == [1]
  end

  test "histograms without a sum retain their count and buckets", %{source: source} do
    event =
      metric_event(source,
        event_message: "latency",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 3,
        bucket_counts: [1, 2],
        explicit_bounds: [1.0]
      )

    series = ingest([event], source, 3)
    assert values_by_name(series)["latency_count"] == [3]
    assert buckets(series, "latency_bucket") == %{"1" => [1], "+Inf" => [3]}
    refute Map.has_key?(values_by_name(series), "latency_sum")
  end

  test "the positive infinity bucket always holds the histogram count", %{source: source} do
    events = [
      metric_event(source,
        event_message: "no_buckets",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 4
      ),
      metric_event(source,
        event_message: "overflow",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 9,
        bucket_counts: [1, 2, 6],
        explicit_bounds: [1.0, 2.0]
      )
    ]

    series = ingest(events, source, 6)
    assert buckets(series, "no_buckets_bucket") == %{"+Inf" => [4]}
    assert buckets(series, "overflow_bucket") == %{"1" => [1], "2" => [3], "+Inf" => [9]}
  end

  @tag config_labels: %{"" => "x", "empty" => ""}
  test "empty config label names and values are omitted", %{source: source} do
    event = metric_event(source, metric_type: "gauge", value: 1, attributes: %{"kept" => "x"})
    assert [series] = ingest([event], source)
    assert series["metric"]["kept"] == "x"
    refute Map.has_key?(series["metric"], "")
    refute Map.has_key?(series["metric"], "empty")
  end

  test "exported labels do not overwrite existing exported attribute names", %{source: source} do
    event =
      metric_event(source,
        metric_type: "gauge",
        value: 1,
        attributes: %{"source" => "client", "exported_source" => "proxy"}
      )

    assert [series] = ingest([event], source)

    assert %{
             "exported_source" => "proxy",
             "exported_exported_source" => "client"
           } = series["metric"]

    assert series["metric"]["source"] == source.name
  end

  @tag config_labels: %{"env" => "prod", "deploy.region" => "eu"}
  test "config labels override attributes and apply to every histogram series", %{source: source} do
    events = [
      metric_event(source,
        event_message: "jobs",
        metric_type: "gauge",
        value: 1,
        attributes: %{"env" => "staging", "queue" => "default"}
      ),
      metric_event(source,
        event_message: "latency",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 1,
        sum: 0.5,
        bucket_counts: [1, 0],
        explicit_bounds: [1.0]
      )
    ]

    series = ingest(events, source, 5)

    assert Enum.all?(series, fn series ->
             series["metric"]["env"] == "prod" and series["metric"]["deploy_region"] == "eu"
           end)

    assert Enum.find(series, &(&1["metric"]["__name__"] == "jobs"))["metric"]["queue"] ==
             "default"
  end

  @tag config_labels: %{"job" => "static", "source" => "static"}
  test "reserved config labels survive under exported names", %{source: source} do
    event = metric_event(source, metric_type: "gauge", value: 1)
    assert [series] = ingest([event], source)
    assert %{"exported_job" => "static", "exported_source" => "static"} = series["metric"]
    assert series["metric"]["source"] == source.name
    refute Map.has_key?(series["metric"], "job")
  end

  test "one series retains multiple samples supplied out of timestamp order", %{source: source} do
    timestamp_ms = System.system_time(:millisecond) - 10_000

    events =
      for offset <- [3_000, 1_000, 2_000] do
        metric_event(source,
          event_message: "ordered",
          metric_type: "gauge",
          value: div(offset, 1_000),
          timestamp: (timestamp_ms + offset) * 1_000_000
        )
      end

    assert {:ok, _} = Backends.ingest_logs(events, source)

    TestUtils.retry_assert(@vm_retry, fn ->
      assert [series] = export(source)

      assert series["timestamps"] == [
               timestamp_ms + 1_000,
               timestamp_ms + 2_000,
               timestamp_ms + 3_000
             ]

      assert series["values"] == [1, 2, 3]
    end)
  end

  test "VM drops NaN samples and preserves infinities alongside a finite control", %{
    source: source
  } do
    events =
      for {name, value} <- [
            {"finite", 1},
            {"nan", :nan},
            {"inf", :infinity},
            {"neg", :negative_infinity},
            {"big", 10 ** 400}
          ] do
        metric_event(source, event_message: name, metric_type: "gauge", value: value)
      end

    assert ingest(events, source, 4) |> values_by_name() == %{
             "finite" => [1],
             "inf" => ["Infinity"],
             "neg" => ["-Infinity"],
             "big" => ["Infinity"]
           }
  end
end
