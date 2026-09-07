# Usage: TAG=before mix run --no-start bench/clickhouse_encode_e2e.exs
#
# End-to-end encode-stage benchmark for the ClickHouse ingester. Exercises the
# real production function `Ingester.encode_batch/2` (NOT hand-replicated
# baselines) over realistic log / metric / trace batches, then forces the
# resulting iodata to a binary so the full encode cost is measured.
#
# Run once per checkout via bench/run_encode_e2e.sh, which tags each run
# (before/after) and saves Benchee results for cross-SHA comparison.

alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
alias Logflare.LogEvent

tag = System.get_env("TAG", "untagged")
save_dir = System.get_env("BENCH_SAVE_DIR", "/tmp")
File.mkdir_p!(save_dir)

batch_size = String.to_integer(System.get_env("BATCH_SIZE", "500"))

mapping_config_id = "00000000-0000-0000-0001-000000000003"
source_uuid = String.to_atom("550e8400-e29b-41d4-a716-446655440000")

# Realistic, *varied* resource attributes: ~12 keys spanning the typical OTEL
# service/host/k8s/cloud namespaces. Key set and value lengths vary by event so
# the map encoder isn't fed an artificially uniform batch.
resource_attributes = fn i ->
  base = %{
    "service.name" => "checkout-service",
    "service.version" => "2.#{rem(i, 20)}.1",
    "service.namespace" => "ecommerce",
    "service.instance.id" => "i-0#{rem(i, 64)}a1b2c3d4e5f6",
    "host.name" => "ip-10-0-#{rem(i, 256)}-#{rem(i * 7, 256)}.ec2.internal",
    "host.arch" => "amd64",
    "cloud.provider" => "aws",
    "cloud.region" => "us-east-1",
    "telemetry.sdk.name" => "opentelemetry",
    "telemetry.sdk.language" => "erlang",
    "telemetry.sdk.version" => "1.4.0"
  }

  cond do
    rem(i, 3) == 0 ->
      Map.merge(base, %{
        "k8s.pod.name" => "checkout-service-#{rem(i, 9999)}-abcde",
        "k8s.namespace.name" => "production",
        "k8s.node.name" => "gke-prod-pool-#{rem(i, 16)}"
      })

    rem(i, 3) == 1 ->
      Map.put(base, "deployment.environment", "production")

    true ->
      base
  end
end

log_attributes = fn i ->
  base = %{
    "http.method" => Enum.at(~w(GET POST PUT DELETE), rem(i, 4)),
    "http.status_code" => "#{Enum.at([200, 201, 400, 404, 500], rem(i, 5))}",
    "http.route" => "/api/v1/orders/:id/line_items",
    "user.id" => "user_#{rem(i, 100_000)}",
    "request.id" => "req_#{i}"
  }

  if rem(i, 4) == 0,
    do: Map.put(base, "error.stack", "** (RuntimeError) boom\n" <> String.duplicate("    at foo/bar:42\n", 6)),
    else: base
end

# Event messages span short access-log lines and larger structured blobs.
event_message = fn i ->
  if rem(i, 5) == 0 do
    ~s({"event":"order.created","order_id":#{i},"items":#{rem(i, 12) + 1},"total_cents":#{i * 137},"currency":"USD","customer":"user_#{rem(i, 100_000)}","note":"#{String.duplicate("x", rem(i, 200))}"})
  else
    "GET /api/v1/orders/#{i}/line_items 200 in #{rem(i, 800)}ms"
  end
end

base_event = fn type, body ->
  %LogEvent{
    id: Ecto.UUID.generate(),
    source_uuid: source_uuid,
    source_name: "bench source",
    event_type: type,
    ingested_at: DateTime.utc_now(),
    body: Map.merge(body, %{"mapping_config_id" => mapping_config_id})
  }
end

log_event = fn i ->
  base_event.(:log, %{
    "project" => "bench-project",
    "trace_id" => Base.encode16(<<i::128>>, case: :lower),
    "span_id" => Base.encode16(<<i::64>>, case: :lower),
    "trace_flags" => 1,
    "severity_text" => Enum.at(~w(DEBUG INFO WARN ERROR), rem(i, 4)),
    "severity_number" => Enum.at([5, 9, 13, 17], rem(i, 4)),
    "service_name" => "checkout-service",
    "event_message" => event_message.(i),
    "scope_name" => "logflare.instrumentation",
    "scope_version" => "1.0.0",
    "scope_schema_url" => "",
    "resource_schema_url" => "https://opentelemetry.io/schemas/1.21.0",
    "resource_attributes" => resource_attributes.(i),
    "scope_attributes" => %{"library.language" => "elixir"},
    "log_attributes" => log_attributes.(i),
    "timestamp" => 1_700_000_000_000_000 + i
  })
end

metric_event = fn i ->
  base_event.(:metric, %{
    "project" => "bench-project",
    "time_unix" => 1_700_000_000_000_000 + i,
    "start_time_unix" => 1_700_000_000_000_000,
    "metric_name" => "http.server.duration",
    "metric_description" => "Duration of inbound HTTP requests",
    "metric_unit" => "ms",
    "metric_type" => rem(i, 3) + 1,
    "service_name" => "checkout-service",
    "event_message" => "",
    "scope_name" => "logflare.instrumentation",
    "scope_version" => "1.0.0",
    "scope_schema_url" => "",
    "resource_schema_url" => "https://opentelemetry.io/schemas/1.21.0",
    "resource_attributes" => resource_attributes.(i),
    "scope_attributes" => %{"library.language" => "elixir"},
    "attributes" => log_attributes.(i),
    "aggregation_temporality" => "cumulative",
    "is_monotonic" => true,
    "flags" => 0,
    "value" => i * 1.5,
    "count" => i,
    "sum" => i * 12.3,
    "min" => 0.5,
    "max" => i * 1.0,
    "scale" => 0,
    "zero_count" => 0,
    "positive_offset" => 0,
    "negative_offset" => 0,
    "bucket_counts" => Enum.map(0..9, &(&1 + rem(i, 3))),
    "explicit_bounds" => [0.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0],
    "positive_bucket_counts" => [],
    "negative_bucket_counts" => [],
    "quantile_values" => [],
    "quantiles" => [],
    "exemplars.filtered_attributes" => [%{"trace_id" => "abc"}],
    "exemplars.time_unix" => [1_700_000_000_000_000 + i],
    "exemplars.value" => [i * 1.0],
    "exemplars.span_id" => [Base.encode16(<<i::64>>, case: :lower)],
    "exemplars.trace_id" => [Base.encode16(<<i::128>>, case: :lower)],
    "timestamp" => 1_700_000_000_000_000 + i
  })
end

trace_event = fn i ->
  event_count = rem(i, 4)

  base_event.(:trace, %{
    "project" => "bench-project",
    "trace_id" => Base.encode16(<<i::128>>, case: :lower),
    "span_id" => Base.encode16(<<i::64>>, case: :lower),
    "parent_span_id" => Base.encode16(<<i + 1::64>>, case: :lower),
    "trace_state" => "",
    "span_name" => "POST /api/v1/orders",
    "span_kind" => "SPAN_KIND_SERVER",
    "service_name" => "checkout-service",
    "event_message" => "",
    "duration" => rem(i, 800) * 1_000_000,
    "status_code" => Enum.at(~w(STATUS_CODE_OK STATUS_CODE_ERROR), rem(i, 2)),
    "status_message" => "",
    "scope_name" => "logflare.instrumentation",
    "scope_version" => "1.0.0",
    "resource_attributes" => resource_attributes.(i),
    "span_attributes" => log_attributes.(i),
    "events.timestamp" => Enum.map(1..event_count//1, &(1_700_000_000_000_000 + i + &1)),
    "events.name" => Enum.map(1..event_count//1, &"event.#{&1}"),
    "events.attributes" => Enum.map(1..event_count//1, fn n -> %{"exception.type" => "Error#{n}", "exception.message" => "boom #{n}"} end),
    "links.trace_id" => [Base.encode16(<<i + 2::128>>, case: :lower)],
    "links.span_id" => [Base.encode16(<<i + 2::64>>, case: :lower)],
    "links.trace_state" => [""],
    "links.attributes" => [%{"link.kind" => "child"}],
    "timestamp" => 1_700_000_000_000_000 + i
  })
end

log_batch = Enum.map(1..batch_size, log_event)
metric_batch = Enum.map(1..batch_size, metric_event)
trace_batch = Enum.map(1..batch_size, trace_event)

# Encode once and report the produced byte sizes so before/after runs are
# confirmed to be encoding the same volume of data.
for {label, batch, type} <- [{"log", log_batch, :log}, {"metric", metric_batch, :metric}, {"trace", trace_batch, :trace}] do
  bytes = batch |> Ingester.encode_batch(type) |> IO.iodata_to_binary() |> byte_size()
  IO.puts("#{label}: #{batch_size} events -> #{bytes} bytes (#{Float.round(bytes / batch_size, 1)} bytes/event)")
end

IO.puts("")

Benchee.run(
  %{
    "log" => fn -> log_batch |> Ingester.encode_batch(:log) |> IO.iodata_to_binary() end,
    "metric" => fn -> metric_batch |> Ingester.encode_batch(:metric) |> IO.iodata_to_binary() end,
    "trace" => fn -> trace_batch |> Ingester.encode_batch(:trace) |> IO.iodata_to_binary() end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  save: [path: Path.join(save_dir, "encode_e2e_#{tag}.benchee"), tag: tag],
  print: [configuration: false]
)
