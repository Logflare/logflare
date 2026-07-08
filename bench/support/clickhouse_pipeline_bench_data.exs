defmodule Logflare.Bench.ClickHousePipelineData do
  @moduledoc false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Ingester
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent
  alias Logflare.Mapper

  @mapping_config_id "00000000-0000-0000-0001-000000000003"
  @source_uuid String.to_atom("550e8400-e29b-41d4-a716-446655440000")

  @type event_type :: :log | :metric | :trace

  @spec compiled(event_type()) :: {reference(), String.t()}
  def compiled(type) do
    {type |> MappingDefaults.for_type() |> Mapper.compile!(), MappingDefaults.config_id(type)}
  end

  @spec batch(event_type(), pos_integer(), atom()) :: [LogEvent.t()]
  def batch(type, count, shape \\ :realistic) when type in [:log, :metric, :trace] do
    Enum.map(1..count, &event(type, &1, shape))
  end

  @spec encoded_bytes([LogEvent.t()], event_type(), reference(), String.t()) :: non_neg_integer()
  def encoded_bytes(events, type, compiled, config_id) do
    events
    |> Enum.map(&map_event(&1, type, compiled, config_id))
    |> Ingester.encode_batch(type)
    |> IO.iodata_to_binary()
    |> byte_size()
  end

  @spec old_encode_gzip([LogEvent.t()], event_type(), reference(), String.t()) :: binary()
  def old_encode_gzip(events, type, compiled, config_id) do
    events
    |> Enum.map(&map_event(&1, type, compiled, config_id))
    |> Ingester.encode_batch(type)
    |> :zlib.gzip()
  end

  @spec old_materialize_then_gzip([LogEvent.t()], event_type(), reference(), String.t()) ::
          binary()
  def old_materialize_then_gzip(events, type, compiled, config_id) do
    events
    |> Enum.map(&map_event(&1, type, compiled, config_id))
    |> Ingester.encode_batch(type)
    |> IO.iodata_to_binary()
    |> :zlib.gzip()
  end

  @spec stream_deflate_events([LogEvent.t()], event_type(), reference(), String.t()) :: binary()
  def stream_deflate_events(events, type, compiled, config_id) do
    gzip_stream(fn z ->
      Enum.reduce(events, [], fn event, chunks ->
        mapped = map_event(event, type, compiled, config_id)
        [chunks, :zlib.deflate(z, Ingester.encode_row(mapped, type))]
      end)
    end)
  end

  @spec stream_deflate_events_hoisted([LogEvent.t()], event_type(), reference(), String.t()) ::
          binary()
  def stream_deflate_events_hoisted(events, type, compiled, config_id) do
    mapping_config_id = Ingester.encode_mapping_config_id(config_id)

    gzip_stream(fn z ->
      Enum.reduce(events, [], fn event, chunks ->
        mapped = map_event(event, type, compiled, config_id)
        [chunks, :zlib.deflate(z, Ingester.encode_row(mapped, type, mapping_config_id))]
      end)
    end)
  end

  @spec stream_deflate_events_chunked([LogEvent.t()], event_type(), reference(), String.t()) ::
          binary()
  def stream_deflate_events_chunked(events, type, compiled, config_id) do
    mapping_config_id = Ingester.encode_mapping_config_id(config_id)

    gzip_stream_chunked(events, fn event ->
      event
      |> map_event(type, compiled, config_id)
      |> Ingester.encode_row(type, mapping_config_id)
    end)
  end

  @spec setup_processing_ets([LogEvent.t()]) :: :ets.tid()
  def setup_processing_ets(events) do
    tid = :ets.new(:clickhouse_pipeline_bench_queue, [:set, :public, read_concurrency: true])
    claimed_at = System.monotonic_time(:millisecond)

    for event <- events do
      :ets.insert(
        tid,
        {event.id, :processing, event, :erlang.external_size(event.body), 1, claimed_at}
      )
    end

    tid
  end

  @spec stream_deflate_ets([{term(), :ets.tid()}], event_type(), reference(), String.t()) ::
          binary()
  def stream_deflate_ets(id_tid_pairs, type, compiled, config_id) do
    gzip_stream(fn z ->
      Enum.reduce(id_tid_pairs, [], fn {id, tid}, chunks ->
        case IngestEventQueue.lookup_id(tid, id) do
          {^id, :processing, %LogEvent{} = event, _size} ->
            mapped = map_event(event, type, compiled, config_id)
            [chunks, :zlib.deflate(z, Ingester.encode_row(mapped, type))]

          _ ->
            chunks
        end
      end)
    end)
  end

  @spec stream_deflate_ets_hoisted([{term(), :ets.tid()}], event_type(), reference(), String.t()) ::
          binary()
  def stream_deflate_ets_hoisted(id_tid_pairs, type, compiled, config_id) do
    mapping_config_id = Ingester.encode_mapping_config_id(config_id)

    gzip_stream(fn z ->
      Enum.reduce(id_tid_pairs, [], fn {id, tid}, chunks ->
        case IngestEventQueue.lookup_id(tid, id) do
          {^id, :processing, %LogEvent{} = event, _size} ->
            mapped = map_event(event, type, compiled, config_id)
            [chunks, :zlib.deflate(z, Ingester.encode_row(mapped, type, mapping_config_id))]

          _ ->
            chunks
        end
      end)
    end)
  end

  @spec stream_deflate_ets_chunked([{term(), :ets.tid()}], event_type(), reference(), String.t()) ::
          binary()
  def stream_deflate_ets_chunked(id_tid_pairs, type, compiled, config_id) do
    mapping_config_id = Ingester.encode_mapping_config_id(config_id)

    gzip_stream_chunked(id_tid_pairs, fn {id, tid} ->
      case IngestEventQueue.lookup_id(tid, id) do
        {^id, :processing, %LogEvent{} = event, _size} ->
          event
          |> map_event(type, compiled, config_id)
          |> Ingester.encode_row(type, mapping_config_id)

        _ ->
          []
      end
    end)
  end

  @spec ensure_ingest_queue_started() :: :ok
  def ensure_ingest_queue_started do
    case Process.whereis(IngestEventQueue) do
      nil ->
        {:ok, _pid} = IngestEventQueue.start_link([])
        :ok

      _pid ->
        :ok
    end
  end

  @spec setup_queue(pos_integer()) :: {IngestEventQueue.consolidated_table_key(), :ets.tid()}
  def setup_queue(backend_id) do
    ensure_ingest_queue_started()
    key = {:consolidated, backend_id, self()}

    tid =
      case IngestEventQueue.upsert_tid(key) do
        {:ok, tid} -> tid
        {:error, :already_exists, tid} -> tid
      end

    :ets.delete_all_objects(tid)
    {key, tid}
  end

  @spec old_queue_pop_encode_gzip(
          IngestEventQueue.consolidated_table_key(),
          :ets.tid(),
          [LogEvent.t()],
          event_type(),
          reference(),
          String.t()
        ) :: binary()
  def old_queue_pop_encode_gzip(key, tid, events, type, compiled, config_id) do
    :ets.delete_all_objects(tid)
    :ok = IngestEventQueue.add_to_table({key, tid}, events)
    {:ok, popped} = IngestEventQueue.pop_pending(key, length(events))
    old_encode_gzip(popped, type, compiled, config_id)
  end

  @spec id_passing_queue_stream(
          IngestEventQueue.consolidated_table_key(),
          :ets.tid(),
          [LogEvent.t()],
          event_type(),
          reference(),
          String.t()
        ) :: binary()
  def id_passing_queue_stream(key, tid, events, type, compiled, config_id) do
    id_passing_queue_stream(key, tid, events, type, compiled, config_id, :per_row)
  end

  @spec id_passing_queue_stream_hoisted(
          IngestEventQueue.consolidated_table_key(),
          :ets.tid(),
          [LogEvent.t()],
          event_type(),
          reference(),
          String.t()
        ) :: binary()
  def id_passing_queue_stream_hoisted(key, tid, events, type, compiled, config_id) do
    id_passing_queue_stream(key, tid, events, type, compiled, config_id, :hoisted)
  end

  @spec id_passing_queue_stream_chunked(
          IngestEventQueue.consolidated_table_key(),
          :ets.tid(),
          [LogEvent.t()],
          event_type(),
          reference(),
          String.t()
        ) :: binary()
  def id_passing_queue_stream_chunked(key, tid, events, type, compiled, config_id) do
    id_passing_queue_stream(key, tid, events, type, compiled, config_id, :chunked)
  end

  defp id_passing_queue_stream(key, tid, events, type, compiled, config_id, mode) do
    :ets.delete_all_objects(tid)
    :ok = IngestEventQueue.add_to_table({key, tid}, events)
    {:ok, id_size_pairs, ^tid} = IngestEventQueue.take_pending_ids(key, length(events))

    routed_pairs =
      Enum.reduce(id_size_pairs, [], fn {id, _size}, acc ->
        case IngestEventQueue.lookup_id(tid, id) do
          {^id, :processing, %LogEvent{event_type: ^type}, _size} -> [{id, tid} | acc]
          _ -> acc
        end
      end)

    compress_and_delete(routed_pairs, tid, type, compiled, config_id, mode)
  end

  @spec id_passing_queue_stream_metadata(
          IngestEventQueue.consolidated_table_key(),
          :ets.tid(),
          [LogEvent.t()],
          event_type(),
          reference(),
          String.t()
        ) :: binary()
  def id_passing_queue_stream_metadata(key, tid, events, type, compiled, config_id) do
    :ets.delete_all_objects(tid)
    :ok = IngestEventQueue.add_to_table({key, tid}, events)

    {:ok, metadata, ^tid} =
      IngestEventQueue.take_pending_ids_with_metadata(key, length(events))

    routed_pairs =
      Enum.reduce(metadata, [], fn
        {id, _size, ^type, _day_bucket, _freshness}, acc -> [{id, tid} | acc]
        _other, acc -> acc
      end)

    compress_and_delete(routed_pairs, tid, type, compiled, config_id, :hoisted)
  end

  defp compress_and_delete(routed_pairs, tid, type, compiled, config_id, mode) do
    compressed =
      case mode do
        :per_row -> stream_deflate_ets(routed_pairs, type, compiled, config_id)
        :hoisted -> stream_deflate_ets_hoisted(routed_pairs, type, compiled, config_id)
        :chunked -> stream_deflate_ets_chunked(routed_pairs, type, compiled, config_id)
      end

    Enum.each(routed_pairs, fn {id, ^tid} -> IngestEventQueue.delete_id(tid, id) end)

    compressed
  end

  @spec run_scenario(atom(), map()) :: binary()
  def run_scenario(:old, %{events: events, type: type, compiled: compiled, config_id: config_id}) do
    old_encode_gzip(events, type, compiled, config_id)
  end

  def run_scenario(:old_materialized, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id
      }) do
    old_materialize_then_gzip(events, type, compiled, config_id)
  end

  def run_scenario(:stream, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id
      }) do
    stream_deflate_events(events, type, compiled, config_id)
  end

  def run_scenario(:stream_hoisted, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id
      }) do
    stream_deflate_events_hoisted(events, type, compiled, config_id)
  end

  def run_scenario(:stream_chunked, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id
      }) do
    stream_deflate_events_chunked(events, type, compiled, config_id)
  end

  def run_scenario(:stream_ets, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        processing_tid: tid
      }) do
    stream_deflate_ets(Enum.map(events, &{&1.id, tid}), type, compiled, config_id)
  end

  def run_scenario(:stream_ets_hoisted, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        processing_tid: tid
      }) do
    stream_deflate_ets_hoisted(Enum.map(events, &{&1.id, tid}), type, compiled, config_id)
  end

  def run_scenario(:stream_ets_chunked, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        processing_tid: tid
      }) do
    stream_deflate_ets_chunked(Enum.map(events, &{&1.id, tid}), type, compiled, config_id)
  end

  def run_scenario(:old_queue, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        queue_key: key,
        queue_tid: tid
      }) do
    old_queue_pop_encode_gzip(key, tid, events, type, compiled, config_id)
  end

  def run_scenario(:id_queue, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        queue_key: key,
        queue_tid: tid
      }) do
    id_passing_queue_stream(key, tid, events, type, compiled, config_id)
  end

  def run_scenario(:id_queue_hoisted, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        queue_key: key,
        queue_tid: tid
      }) do
    id_passing_queue_stream_hoisted(key, tid, events, type, compiled, config_id)
  end

  def run_scenario(:id_queue_chunked, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        queue_key: key,
        queue_tid: tid
      }) do
    id_passing_queue_stream_chunked(key, tid, events, type, compiled, config_id)
  end

  def run_scenario(:id_queue_metadata, %{
        events: events,
        type: type,
        compiled: compiled,
        config_id: config_id,
        queue_key: key,
        queue_tid: tid
      }) do
    id_passing_queue_stream_metadata(key, tid, events, type, compiled, config_id)
  end

  defp gzip_stream(fun) do
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)
      chunks = fun.(z)
      IO.iodata_to_binary([chunks, :zlib.deflate(z, "", :finish)])
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end

  defp gzip_stream_chunked(items, row_fun) do
    chunk_rows = System.get_env("CHUNK_ROWS", "100") |> String.to_integer()
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

      {chunks, pending, count} =
        Enum.reduce(items, {[], [], 0}, fn item, {chunks, pending, count} ->
          pending = [row_fun.(item) | pending]
          count = count + 1

          if count >= chunk_rows do
            {[chunks, :zlib.deflate(z, :lists.reverse(pending))], [], 0}
          else
            {chunks, pending, count}
          end
        end)

      chunks = if count > 0, do: [chunks, :zlib.deflate(z, :lists.reverse(pending))], else: chunks
      IO.iodata_to_binary([chunks, :zlib.deflate(z, "", :finish)])
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end

  defp map_event(%LogEvent{} = event, type, compiled, config_id) do
    mapped_body =
      event.body
      |> Mapper.map(compiled)
      |> Map.put("mapping_config_id", config_id)
      |> maybe_compute_duration(type)
      |> resolve_severity_number(type)

    %{event | body: mapped_body}
  end

  defp maybe_compute_duration(
         %{"start_time" => start_time, "end_time" => end_time, "duration" => 0} = body,
         :trace
       )
       when is_integer(start_time) and is_integer(end_time) and end_time > start_time do
    %{body | "duration" => end_time - start_time}
  end

  defp maybe_compute_duration(body, _type), do: body

  defp resolve_severity_number(%{"severity_number_alt" => alt} = body, :log)
       when is_integer(alt) and alt > 0 do
    %{body | "severity_number" => alt}
  end

  defp resolve_severity_number(body, _type), do: body

  defp event(:log, i, shape) do
    base_event(:log, i, %{
      "project" => "bench-project",
      "trace_id" => Base.encode16(<<i::128>>, case: :lower),
      "span_id" => Base.encode16(<<i::64>>, case: :lower),
      "trace_flags" => 1,
      "severity_text" => Enum.at(~w(DEBUG INFO WARN ERROR), rem(i, 4)),
      "severity_number" => Enum.at([5, 9, 13, 17], rem(i, 4)),
      "severity_number_alt" => Enum.at([0, 0, 13, 17], rem(i, 4)),
      "service_name" => "checkout-service",
      "event_message" => event_message(i, shape),
      "scope_name" => "logflare.instrumentation",
      "scope_version" => "1.0.0",
      "scope_schema_url" => "",
      "resource_schema_url" => "https://opentelemetry.io/schemas/1.21.0",
      "resource_attributes" => resource_attributes(i, shape),
      "scope_attributes" => %{"library.language" => "elixir"},
      "log_attributes" => log_attributes(i, shape),
      "timestamp" => 1_700_000_000_000_000 + i
    })
  end

  defp event(:metric, i, shape) do
    base_event(:metric, i, %{
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
      "resource_attributes" => resource_attributes(i, shape),
      "scope_attributes" => %{"library.language" => "elixir"},
      "attributes" => log_attributes(i, shape),
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

  defp event(:trace, i, shape) do
    event_count = rem(i, 4)
    start_time = 1_700_000_000_000_000 + i
    end_time = start_time + rem(i, 800) * 1_000_000

    base_event(:trace, i, %{
      "project" => "bench-project",
      "timestamp" => start_time,
      "start_time" => start_time,
      "end_time" => end_time,
      "trace_id" => Base.encode16(<<i::128>>, case: :lower),
      "span_id" => Base.encode16(<<i::64>>, case: :lower),
      "parent_span_id" => Base.encode16(<<i + 1::64>>, case: :lower),
      "trace_state" => "",
      "span_name" => "POST /api/v1/orders",
      "span_kind" => "SPAN_KIND_SERVER",
      "service_name" => "checkout-service",
      "event_message" => "",
      "duration" => 0,
      "status_code" => Enum.at(~w(STATUS_CODE_OK STATUS_CODE_ERROR), rem(i, 2)),
      "status_message" => "",
      "scope_name" => "logflare.instrumentation",
      "scope_version" => "1.0.0",
      "resource_attributes" => resource_attributes(i, shape),
      "span_attributes" => log_attributes(i, shape),
      "events.timestamp" => Enum.map(1..event_count//1, &(start_time + &1)),
      "events.name" => Enum.map(1..event_count//1, &"event.#{&1}"),
      "events.attributes" =>
        Enum.map(1..event_count//1, fn n ->
          %{"exception.type" => "Error#{n}", "exception.message" => "boom #{n}"}
        end),
      "links.trace_id" => [Base.encode16(<<i + 2::128>>, case: :lower)],
      "links.span_id" => [Base.encode16(<<i + 2::64>>, case: :lower)],
      "links.trace_state" => [""],
      "links.attributes" => [%{"link.kind" => "child"}]
    })
  end

  defp base_event(type, i, body) do
    %LogEvent{
      id: Ecto.UUID.generate(),
      source_uuid: @source_uuid,
      source_name: "bench source",
      event_type: type,
      day_bucket: div(1_700_000_000 + i, 86_400),
      ingest_freshness: :fresh,
      ingested_at: DateTime.utc_now(),
      body: Map.put(body, "mapping_config_id", @mapping_config_id)
    }
  end

  defp resource_attributes(i, :small) do
    %{"service.name" => "checkout-service", "host.name" => "host-#{rem(i, 32)}"}
  end

  defp resource_attributes(i, shape) do
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

    base =
      case rem(i, 3) do
        0 ->
          Map.merge(base, %{
            "k8s.pod.name" => "checkout-service-#{rem(i, 9999)}-abcde",
            "k8s.namespace.name" => "production",
            "k8s.node.name" => "gke-prod-pool-#{rem(i, 16)}"
          })

        1 ->
          Map.put(base, "deployment.environment", "production")

        _ ->
          base
      end

    if shape == :large do
      Map.merge(base, %{
        "large.attr.1" => String.duplicate("a", 512),
        "large.attr.2" => String.duplicate("b", 512),
        "large.attr.3" => String.duplicate("c", 512)
      })
    else
      base
    end
  end

  defp log_attributes(i, :small) do
    %{"http.method" => "GET", "http.status_code" => "200", "request.id" => "req_#{i}"}
  end

  defp log_attributes(i, shape) do
    base = %{
      "http.method" => Enum.at(~w(GET POST PUT DELETE), rem(i, 4)),
      "http.status_code" => "#{Enum.at([200, 201, 400, 404, 500], rem(i, 5))}",
      "http.route" => "/api/v1/orders/:id/line_items",
      "user.id" => "user_#{rem(i, 100_000)}",
      "request.id" => "req_#{i}"
    }

    cond do
      shape == :large ->
        Map.put(
          base,
          "error.stack",
          "** (RuntimeError) boom\n" <> String.duplicate("    at foo/bar:42\n", 50)
        )

      rem(i, 4) == 0 ->
        Map.put(
          base,
          "error.stack",
          "** (RuntimeError) boom\n" <> String.duplicate("    at foo/bar:42\n", 6)
        )

      true ->
        base
    end
  end

  defp event_message(i, :small), do: "GET /api/v1/orders/#{i} 200"

  defp event_message(i, :large) do
    ~s({"event":"order.created","order_id":#{i},"items":#{rem(i, 12) + 1},"total_cents":#{i * 137},"currency":"USD","customer":"user_#{rem(i, 100_000)}","note":"#{String.duplicate("x", 2_000 + rem(i, 500))}"})
  end

  defp event_message(i, _shape) do
    if rem(i, 5) == 0 do
      ~s({"event":"order.created","order_id":#{i},"items":#{rem(i, 12) + 1},"total_cents":#{i * 137},"currency":"USD","customer":"user_#{rem(i, 100_000)}","note":"#{String.duplicate("x", rem(i, 200))}"})
    else
      "GET /api/v1/orders/#{i}/line_items 200 in #{rem(i, 800)}ms"
    end
  end
end
