defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.OtelTest do
  use Logflare.DataCase, async: false

  import Logflare.VictoriaMetricsTestHelpers

  alias Logflare.Logs.OtelMetric
  alias Logflare.Logs.Processor
  alias Logflare.Sources.Source
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Metrics.V1.Gauge
  alias Opentelemetry.Proto.Metrics.V1.Histogram
  alias Opentelemetry.Proto.Metrics.V1.HistogramDataPoint
  alias Opentelemetry.Proto.Metrics.V1.Metric
  alias Opentelemetry.Proto.Metrics.V1.NumberDataPoint
  alias Opentelemetry.Proto.Metrics.V1.ResourceMetrics
  alias Opentelemetry.Proto.Metrics.V1.ScopeMetrics
  alias Opentelemetry.Proto.Metrics.V1.Sum
  alias Opentelemetry.Proto.Resource.V1.Resource

  @moduletag :victoria_metrics
  @vm_retry [sleep: 250, duration: 30_000]

  setup_all :ensure_vm_available
  setup :setup_vm

  setup do
    [timestamp_ns: (System.system_time(:millisecond) - 10_000) * 1_000_000]
  end

  test "one OTLP request fans out across resources, scopes, metrics and data points", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    resources = [
      resource(
        [
          scope("http", [
            gauge("requests", [
              number_point(1, timestamp_ns),
              number_point(2, timestamp_ns + 1_000_000_000)
            ])
          ]),
          scope("jobs", [gauge("workers", [number_point(3, timestamp_ns)])])
        ],
        %{"service.name" => "checkout"}
      ),
      resource(
        [
          scope("http", [
            gauge("requests", [
              number_point(4, timestamp_ns, %{"method" => "GET"}),
              number_point(5, timestamp_ns, %{"method" => "POST"})
            ])
          ])
        ],
        %{"service.name" => "search"}
      )
    ]

    assert {:ok, 5} = submit_otlp(resources, source)

    TestUtils.retry_assert(@vm_retry, fn ->
      series = export(source)
      assert length(series) == 4

      actual =
        Map.new(series, fn series ->
          labels = series["metric"]

          {{labels["job"], labels["__name__"], labels["method"]},
           Enum.zip(series["timestamps"], series["values"])}
        end)

      timestamp_ms = div(timestamp_ns, 1_000_000)

      assert actual == %{
               {"checkout", "requests", nil} => [{timestamp_ms, 1}, {timestamp_ms + 1_000, 2}],
               {"checkout", "workers", nil} => [{timestamp_ms, 3}],
               {"search", "requests", "GET"} => [{timestamp_ms, 4}],
               {"search", "requests", "POST"} => [{timestamp_ms, 5}]
             }
    end)
  end

  test "protobuf integer and double points preserve signed values and nanosecond timestamps", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    points = [
      number_point(-7, timestamp_ns + 999_999, %{"kind" => "integer"}),
      number_point(-0.125, timestamp_ns + 1_123_456, %{"kind" => "double"})
    ]

    resources = [resource([scope("numbers", [gauge("signed", points)])])]
    series = ingest_otlp(resources, source, 2)
    by_kind = Map.new(series, &{&1["metric"]["kind"], &1})

    assert by_kind["integer"]["values"] == [-7]
    assert by_kind["integer"]["timestamps"] == [div(timestamp_ns, 1_000_000)]
    assert by_kind["double"]["values"] == [-0.125]
    assert by_kind["double"]["timestamps"] == [div(timestamp_ns, 1_000_000) + 1]
  end

  test "cumulative counters retain a reset across separate OTLP requests", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    first =
      number_point(50, timestamp_ns, %{}, start_time_unix_nano: timestamp_ns - 60_000_000_000)

    reset =
      number_point(3, timestamp_ns + 2_000_000_000, %{},
        start_time_unix_nano: timestamp_ns + 1_000_000_000
      )

    first_request = [resource([scope("counter", [sum_metric("requests_total", [first])])])]
    reset_request = [resource([scope("counter", [sum_metric("requests_total", [reset])])])]

    assert [%{"values" => [50]}] = ingest_otlp(first_request, source)
    assert {:ok, _} = submit_otlp(reset_request, source)

    TestUtils.retry_assert(@vm_retry, fn ->
      assert [series] = export(source)
      assert series["values"] == [50, 3]

      assert series["timestamps"] == [
               div(timestamp_ns, 1_000_000),
               div(timestamp_ns, 1_000_000) + 2_000
             ]
    end)
  end

  test "protobuf zero histograms retain zero counts, buckets and an explicitly present sum", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    point = %HistogramDataPoint{
      time_unix_nano: timestamp_ns,
      count: 0,
      sum: 0.0,
      bucket_counts: [0, 0, 0],
      explicit_bounds: [0.0, 1.0]
    }

    resources = [resource([scope("histograms", [histogram("empty", [point])])])]
    series = ingest_otlp(resources, source, 5)

    assert values_by_name(series)["empty_count"] == [0]
    assert values_by_name(series)["empty_sum"] == [0]
    assert buckets(series, "empty_bucket") == %{"0" => [0], "1" => [0], "+Inf" => [0]}
  end

  test "protobuf histograms without buckets or sum retain only count and infinity bucket", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    point = %HistogramDataPoint{time_unix_nano: timestamp_ns, count: 4, min: 2.0, max: 9.0}
    resources = [resource([scope("histograms", [histogram("unbucketed", [point])])])]
    series = ingest_otlp(resources, source, 2)

    assert values_by_name(series) == %{"unbucketed_count" => [4], "unbucketed_bucket" => [4]}
    assert buckets(series, "unbucketed_bucket") == %{"+Inf" => [4]}
  end

  test "histogram data points keep independent attributes and optional sum presence", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    points = [
      %HistogramDataPoint{
        time_unix_nano: timestamp_ns,
        attributes: attributes(%{"queue" => "default"}),
        count: 3,
        sum: 2.5,
        bucket_counts: [1, 2],
        explicit_bounds: [1.0]
      },
      %HistogramDataPoint{
        time_unix_nano: timestamp_ns,
        attributes: attributes(%{"queue" => "priority"}),
        count: 2,
        bucket_counts: [0, 2],
        explicit_bounds: [1.0]
      }
    ]

    resources = [resource([scope("histograms", [histogram("latency", points)])])]
    series = ingest_otlp(resources, source, 7)
    by_queue = Enum.group_by(series, & &1["metric"]["queue"])

    assert values_by_name(by_queue["default"])["latency_sum"] == [2.5]
    assert buckets(by_queue["default"], "latency_bucket") == %{"1" => [1], "+Inf" => [3]}
    refute Map.has_key?(values_by_name(by_queue["priority"]), "latency_sum")
    assert buckets(by_queue["priority"], "latency_bucket") == %{"1" => [0], "+Inf" => [2]}
  end

  test "OTLP unspecified and delta temporalities do not discard a cumulative control", %{
    source: source,
    backend: backend,
    timestamp_ns: timestamp_ns
  } do
    point = number_point(7, timestamp_ns)

    metrics = [
      sum_metric("unspecified", [point],
        aggregation_temporality: :AGGREGATION_TEMPORALITY_UNSPECIFIED
      ),
      sum_metric("delta", [point], aggregation_temporality: :AGGREGATION_TEMPORALITY_DELTA),
      sum_metric("cumulative_total", [point])
    ]

    resources = [resource([scope("temporalities", metrics)])]

    series = ingest_otlp(resources, source)
    assert_drops(backend, %{non_cumulative: 2})

    assert values_by_name(series) == %{
             "cumulative_total" => [7]
           }
  end

  test "VM drops protobuf NaN samples and retains infinities beside a finite control", %{
    source: source,
    timestamp_ns: timestamp_ns
  } do
    metrics =
      for {name, value} <- [finite: 1, nan: :nan, inf: :infinity, neg: :negative_infinity] do
        gauge(Atom.to_string(name), [number_point(value, timestamp_ns)])
      end

    resources = [resource([scope("specials", metrics)])]

    assert ingest_otlp(resources, source, 3) |> values_by_name() == %{
             "finite" => [1],
             "inf" => ["Infinity"],
             "neg" => ["-Infinity"]
           }
  end

  @spec ingest_otlp([ResourceMetrics.t()], Source.t(), pos_integer()) :: [map()]
  defp ingest_otlp(resources, source, count \\ 1) do
    assert {:ok, _} = submit_otlp(resources, source)
    wait_for_series(source, count)
  end

  @spec submit_otlp([ResourceMetrics.t()], Source.t()) ::
          {:ok, non_neg_integer()} | {:error, term()}
  defp submit_otlp(resources, source) do
    request =
      %ExportMetricsServiceRequest{resource_metrics: resources}
      |> ExportMetricsServiceRequest.encode()
      |> ExportMetricsServiceRequest.decode()

    Processor.ingest(request.resource_metrics, OtelMetric, source)
  end

  @spec resource([ScopeMetrics.t()], map()) :: ResourceMetrics.t()
  defp resource(scopes, attrs \\ %{}) do
    %ResourceMetrics{resource: %Resource{attributes: attributes(attrs)}, scope_metrics: scopes}
  end

  @spec scope(String.t(), [Metric.t()]) :: ScopeMetrics.t()
  defp scope(name, metrics) do
    %ScopeMetrics{scope: %InstrumentationScope{name: name, version: "1.0"}, metrics: metrics}
  end

  @spec gauge(String.t(), [NumberDataPoint.t()]) :: Metric.t()
  defp gauge(name, points), do: %Metric{name: name, data: {:gauge, %Gauge{data_points: points}}}

  @spec sum_metric(String.t(), [NumberDataPoint.t()], keyword()) :: Metric.t()
  defp sum_metric(name, points, opts \\ []) do
    sum =
      struct!(
        Sum,
        Keyword.merge(
          [
            data_points: points,
            is_monotonic: true,
            aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
          ],
          opts
        )
      )

    %Metric{name: name, data: {:sum, sum}}
  end

  @spec histogram(String.t(), [HistogramDataPoint.t()]) :: Metric.t()
  defp histogram(name, points) do
    %Metric{
      name: name,
      data:
        {:histogram,
         %Histogram{
           data_points: points,
           aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
         }}
    }
  end

  @spec number_point(
          number() | :nan | :infinity | :negative_infinity,
          non_neg_integer(),
          map(),
          keyword()
        ) ::
          NumberDataPoint.t()
  defp number_point(value, timestamp_ns, attrs \\ %{}, opts \\ []) do
    value = if is_integer(value), do: {:as_int, value}, else: {:as_double, value}

    struct!(
      NumberDataPoint,
      Keyword.merge(
        [value: value, time_unix_nano: timestamp_ns, attributes: attributes(attrs)],
        opts
      )
    )
  end

  @spec attributes(map()) :: [KeyValue.t()]
  defp attributes(attrs) do
    Enum.map(attrs, fn {key, value} ->
      %KeyValue{key: key, value: %AnyValue{value: {:string_value, value}}}
    end)
  end
end
