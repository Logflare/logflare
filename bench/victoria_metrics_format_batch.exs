# Benchmark for VictoriaMetricsAdaptor.format_batch/1: OTEL metric LogEvents to a
# snappy-compressed Prometheus remote write payload. Covers label building, series
# grouping, protobuf encoding and compression. It excludes Broadway, HTTP and VM.
#
# format_batch/1 resolves source names through Sources.Cache, so the app is started
# against the test database (run `MIX_ENV=test mix ecto.setup` first). Fixtures are
# inserted inside a shared sandbox checkout, so nothing is committed.
#
# Usage:
#
#   MIX_ENV=test mix run bench/victoria_metrics_format_batch.exs
#
#   # compare two revisions: save on the first, load on the second
#   BENCH_SAVE=/tmp/vm_before.benchee BENCH_TAG=before \
#     MIX_ENV=test mix run bench/victoria_metrics_format_batch.exs
#   BENCH_LOAD=/tmp/vm_before.benchee \
#     MIX_ENV=test mix run bench/victoria_metrics_format_batch.exs
#
# BENCH_TIME and BENCH_WARMUP override the per-scenario seconds (defaults 5 and 2).

alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
alias Logflare.Factory

:ok = Ecto.Adapters.SQL.Sandbox.checkout(Logflare.Repo)
Ecto.Adapters.SQL.Sandbox.mode(Logflare.Repo, {:shared, self()})

Factory.insert(:plan)
source = Factory.insert(:source, user: Factory.insert(:user), name: "bench-service")
now_ns = System.system_time(:nanosecond)

attributes = fn i ->
  %{
    "http.method" => Enum.at(["GET", "POST", "PUT"], rem(i, 3)),
    "http.route" => "/api/v1/resource/#{rem(i, 20)}",
    "http.status_code" => Enum.at([200, 201, 404, 500], rem(i, 4)),
    "net.host.name" => "host-#{rem(i, 5)}",
    "deployment.environment" => "production"
  }
end

resource = %{
  "service.name" => "checkout",
  "service.namespace" => "shop",
  "service.instance.id" => "checkout-7d9f"
}

base = fn i, extra ->
  Map.merge(
    %{
      source: source,
      event_message: "http.server.request.duration",
      metadata: %{"type" => "metric"},
      timestamp: now_ns + i * 1_000_000,
      attributes: attributes.(i),
      resource: resource,
      aggregation_temporality: "cumulative"
    },
    extra
  )
end

gauge = fn i -> Factory.build(:log_event, base.(i, %{metric_type: "gauge", value: i * 1.5})) end

histogram = fn i ->
  bounds = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0]
  counts = Enum.map(0..length(bounds), &rem(i + &1, 7))

  Factory.build(
    :log_event,
    base.(i, %{
      metric_type: "histogram",
      count: Enum.sum(counts),
      sum: i * 0.75,
      bucket_counts: counts,
      explicit_bounds: bounds
    })
  )
end

batch_size = String.to_integer(System.get_env("BATCH_SIZE", "250"))

inputs = %{
  "#{batch_size} gauges" => Enum.map(1..batch_size, gauge),
  "#{batch_size} histograms (12 buckets)" => Enum.map(1..batch_size, histogram),
  "#{batch_size} mixed (80% gauges)" =>
    Enum.map(1..batch_size, fn i -> if rem(i, 5) == 0, do: histogram.(i), else: gauge.(i) end)
}

benchee_opts =
  [
    inputs: inputs,
    time: String.to_integer(System.get_env("BENCH_TIME", "5")),
    warmup: String.to_integer(System.get_env("BENCH_WARMUP", "2")),
    memory_time: 1
  ]
  |> then(fn opts ->
    case System.get_env("BENCH_SAVE") do
      nil -> opts
      path -> Keyword.put(opts, :save, path: path, tag: System.get_env("BENCH_TAG", "saved"))
    end
  end)
  |> then(fn opts ->
    case System.get_env("BENCH_LOAD") do
      nil -> opts
      path -> Keyword.put(opts, :load, path)
    end
  end)

Benchee.run(%{"format_batch/1" => &VictoriaMetricsAdaptor.format_batch/1}, benchee_opts)
