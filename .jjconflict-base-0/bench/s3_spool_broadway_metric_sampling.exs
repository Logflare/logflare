# Fixed-work S3 spool producer throughput benchmark.
#
# This drives the real Spool.ProducerPipeline topology, BufferProducer,
# IngestEventQueue pointer path, ETF encoding, gzip compression, and ack cleanup.
# Storage and queue network calls are replaced by an in-memory sink so the result
# isolates local spool/Broadway CPU throughput rather than S3/SQS latency.
#
# Run through bench/run_s3_spool_broadway_metric_sampling.sh.

defmodule Logflare.Bench.S3SpoolMetricSampling do
  @moduledoc false

  alias Broadway.Topology.ProcessorStage
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.ProducerPipeline
  alias Logflare.Bench.S3SpoolSink
  alias Logflare.LogEvent

  @pipeline_name Logflare.Bench.S3SpoolMetricSamplingPipeline
  @metric_exporter_name Logflare.Bench.S3SpoolProductionMetrics
  @processor_message_event [:broadway, :processor, :message, :stop]
  @expected_broadway_events [
    [:broadway, :batch_processor, :stop],
    [:broadway, :batcher, :stop],
    @processor_message_event,
    [:broadway, :processor, :stop]
  ]
  @wait_timeout_ms 30_000

  @spec run() :: :ok
  def run do
    Code.require_file("support/s3_spool_bench_sink.exs", __DIR__)

    condition = System.get_env("CONDITION", "unknown")
    handler_store = choice_env!("TELEMETRY_HANDLER_STORE", "ets", ["ets", "persistent"])

    message_sample_denominator = message_sample_denominator!()

    manual_batch_flush = choice_env!("MANUAL_BATCH_FLUSH", "false", ["false", "true"])

    events_per_trial = positive_env!("EVENTS", 100_000)
    warmups = non_negative_env!("WARMUPS", 4)
    trials = positive_env!("TRIALS", 7)
    payload_bytes = non_negative_env!("PAYLOAD_BYTES", 256)
    batch_timeout = positive_env!("BATCH_TIMEOUT_MS", 100)
    order_index = non_negative_env!("ORDER_INDEX", 0)

    {:ok, _} = Application.ensure_all_started(:broadway)
    {:module, ProcessorStage} = Code.ensure_loaded(ProcessorStage)
    Logger.configure(level: :error)
    :erlang.system_flag(:scheduler_wall_time, true)
    handler_backend = configure_handler_store!(handler_store)

    compiled_condition =
      if function_exported?(ProcessorStage, :__telemetry_enabled__?, 0),
        do: "telemetry_off",
        else: "telemetry_on"

    if condition in ["telemetry_on", "telemetry_off"] and condition != compiled_condition do
      raise "requested #{condition}, but compiled Broadway variant is #{compiled_condition}"
    end

    Application.put_env(:logflare, :env, :dev)

    Application.put_env(:logflare, :spool,
      bucket: "benchmark-bucket",
      partitions: 4,
      batch_timeout: batch_timeout,
      compress: true,
      format: :etf,
      queue_name: "benchmark-queue",
      storage_mod: S3SpoolSink,
      queue_mod: S3SpoolSink
    )

    :persistent_term.put(
      {MemoryMonitor, :stats},
      %{
        throttled?: false,
        total_percent: 0.0,
        total_limit_percent: 1.0,
        ets_percent: 0.0,
        ets_limit_percent: 1.0,
        consumer_throttled?: false
      }
    )

    handler_info = configure_broadway_handlers!(message_sample_denominator)

    S3SpoolSink.reset()

    {:ok, _in_flight_registry} =
      Registry.start_link(keys: :unique, name: BufferProducer.InFlightRegistry)

    {:ok, _queue_manager} = IngestEventQueue.start_link([])
    {:ok, _pipeline} = ProducerPipeline.start_link(name: @pipeline_name)

    [producer_name] = Broadway.producer_names(@pipeline_name)
    producer_pid = GenServer.whereis(producer_name)
    queue_tid = await_queue!(producer_pid)

    batcher_pids =
      if manual_batch_flush == "true", do: batcher_pids!(@pipeline_name), else: []

    :ok = :sys.suspend(producer_pid)

    broadway_version = Application.spec(:broadway, :vsn) |> to_string()
    telemetry_version = Application.spec(:telemetry, :vsn) |> to_string()

    IO.puts(
      "config condition=#{condition} compiled_variant=#{compiled_condition} " <>
        "handler_store=#{handler_store} handler_backend=#{handler_backend} " <>
        "message_sample_denominator=#{format_denominator(message_sample_denominator)} " <>
        "configured_message_sample_rate_percent=" <>
        "#{format_sample_rate_percent(message_sample_denominator)} " <>
        "order_index=#{order_index} manual_batch_flush=#{manual_batch_flush} " <>
        "broadway=#{broadway_version} telemetry=#{telemetry_version} " <>
        "otp=#{System.otp_release()} schedulers=#{System.schedulers_online()} " <>
        "scheduler_wall_time=true lock_counting=#{:erlang.system_info(:lock_counting)} " <>
        "events=#{events_per_trial} warmups=#{warmups} trials=#{trials} " <>
        "payload_bytes=#{payload_bytes} batch_timeout_ms=#{batch_timeout} " <>
        "format=etf compress=true storage=in_memory"
    )

    print_handler_info(handler_info)

    if warmups > 0 do
      Enum.each(1..warmups, fn index ->
        run_trial(
          :warmup,
          index,
          events_per_trial,
          payload_bytes,
          producer_pid,
          queue_tid,
          batcher_pids
        )
      end)
    end

    results =
      Enum.map(1..trials, fn index ->
        run_trial(
          :measurement,
          index,
          events_per_trial,
          payload_bytes,
          producer_pid,
          queue_tid,
          batcher_pids
        )
      end)

    throughputs = Enum.map(results, & &1.events_per_second)
    reductions_per_event = Enum.map(results, & &1.reductions_per_event)

    IO.puts(
      "summary condition=#{condition} handler_store=#{handler_store} " <>
        "message_sample_denominator=#{format_denominator(message_sample_denominator)} samples=#{trials} " <>
        "median_events_per_second=#{round2(median(throughputs))} " <>
        "mean_events_per_second=#{round2(mean(throughputs))} " <>
        "stdev_events_per_second=#{round2(sample_stdev(throughputs))} " <>
        "median_reductions_per_event=#{round2(median(reductions_per_event))}"
    )

    print_metric_store!(handler_info)
    :ok
  end

  defp run_trial(
         kind,
         index,
         event_count,
         payload_bytes,
         producer_pid,
         queue_tid,
         batcher_pids
       ) do
    before_sink = S3SpoolSink.snapshot()
    prepare_events!(index, event_count, payload_bytes, producer_pid, queue_tid)

    target_events = before_sink.events + event_count
    scheduler_before = scheduler_wall_time_snapshot!()
    {reductions_before, _} = :erlang.statistics(:reductions)
    started_at = System.monotonic_time(:microsecond)

    :ok = :sys.resume(producer_pid)
    flush_pending_batches!(batcher_pids, target_events, queue_tid)

    deadline = System.monotonic_time(:millisecond) + @wait_timeout_ms
    await_trial_complete!(target_events, producer_pid, queue_tid, deadline)

    elapsed_us = System.monotonic_time(:microsecond) - started_at
    {reductions_after, _} = :erlang.statistics(:reductions)
    scheduler_after = scheduler_wall_time_snapshot!()
    scheduler = scheduler_wall_time_delta!(scheduler_before, scheduler_after)
    :ok = :sys.suspend(producer_pid)

    after_sink = S3SpoolSink.snapshot()
    output_events = after_sink.events - before_sink.events

    if output_events != event_count do
      raise "S3 spool benchmark produced #{output_events} events, expected exactly #{event_count}"
    end

    bytes = after_sink.bytes - before_sink.bytes
    files = after_sink.files - before_sink.files
    reductions = reductions_after - reductions_before
    events_per_second = event_count * 1_000_000 / elapsed_us
    reductions_per_event = reductions / event_count
    events_per_active_scheduler_second = event_count * 1_000_000 / scheduler.active_us

    IO.puts(
      "sample kind=#{kind} run=#{index} events=#{event_count} output_events=#{output_events} " <>
        "elapsed_us=#{elapsed_us} events_per_second=#{round2(events_per_second)} " <>
        "reductions=#{reductions} reductions_per_event=#{round2(reductions_per_event)} " <>
        "bytes=#{bytes} files=#{files} " <>
        "scheduler_active_us=#{scheduler.active_us} scheduler_total_us=#{scheduler.total_us} " <>
        "scheduler_utilization_percent=#{round2(scheduler.utilization_percent)} " <>
        "average_active_schedulers=#{round2(scheduler.average_active_schedulers)} " <>
        "events_per_active_scheduler_second=#{round2(events_per_active_scheduler_second)}"
    )

    %{
      events_per_second: events_per_second,
      reductions_per_event: reductions_per_event
    }
  end

  defp await_trial_complete!(target_events, producer_pid, queue_tid, deadline) do
    sink_events = S3SpoolSink.snapshot().events
    in_flight = BufferProducer.in_flight_count(producer_pid)
    queue_size = :ets.info(queue_tid, :size)

    generation_size =
      {:spool_producer, nil}
      |> IngestEventQueue.list_generations()
      |> Enum.map(fn {tid, _created_at} -> :ets.info(tid, :size) end)
      |> Enum.sum()

    cond do
      sink_events > target_events ->
        raise "S3 spool benchmark overproduced events: sink_events=#{sink_events} target=#{target_events}"

      sink_events == target_events and in_flight == 0 and queue_size == 0 and
          generation_size == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "benchmark timed out waiting for pipeline progress: " <>
                "sink_events=#{sink_events} target=#{target_events} in_flight=#{in_flight} " <>
                "queue_size=#{queue_size} generation_size=#{generation_size}"

      true ->
        receive do
        after
          1 -> await_trial_complete!(target_events, producer_pid, queue_tid, deadline)
        end
    end
  end

  defp flush_pending_batches!([], _target_events, _queue_tid), do: :ok

  defp flush_pending_batches!(batcher_pids, target_events, queue_tid) do
    deadline = System.monotonic_time(:millisecond) + @wait_timeout_ms
    await_flush_ready!(batcher_pids, target_events, queue_tid, deadline)
    Enum.each(batcher_pids, &trigger_batcher_timeouts!/1)
  end

  defp await_flush_ready!(batcher_pids, target_events, queue_tid, deadline) do
    delivered_events = S3SpoolSink.snapshot().events
    pending_results = Enum.map(batcher_pids, &batcher_pending_events/1)
    queue_size = :ets.info(queue_tid, :size)

    ready? =
      case Enum.find(pending_results, &(&1 == :busy)) do
        nil ->
          pending_events = pending_results |> Enum.map(fn {:ok, count} -> count end) |> Enum.sum()
          delivered_events + pending_events == target_events and queue_size == 0

        :busy ->
          false
      end

    cond do
      ready? ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "manual batch flush timed out: " <>
                "delivered=#{delivered_events} pending=#{inspect(pending_results)} " <>
                "target=#{target_events} queue_size=#{queue_size}"

      true ->
        receive do
        after
          1 -> await_flush_ready!(batcher_pids, target_events, queue_tid, deadline)
        end
    end
  end

  defp trigger_batcher_timeouts!(pid) do
    {:dictionary, dictionary} = Process.info(pid, :dictionary)

    {Broadway.Topology.BatcherStage.All, batches} =
      List.keyfind(dictionary, Broadway.Topology.BatcherStage.All, 0)

    Enum.each(batches, fn {ref, batch_key} ->
      {^batch_key, {_current, _state, _splitter, timer, ^ref}} =
        List.keyfind(dictionary, batch_key, 0)

      send(pid, {:timeout, timer, ref})
    end)
  end

  defp batcher_pending_events(pid) do
    {:dictionary, dictionary} = Process.info(pid, :dictionary)

    case List.keyfind(dictionary, Broadway.Topology.BatcherStage.All, 0) do
      {Broadway.Topology.BatcherStage.All, batches} ->
        Enum.reduce_while(Map.values(batches), {:ok, 0}, fn batch_key, {:ok, total} ->
          case List.keyfind(dictionary, batch_key, 0) do
            {^batch_key, {current, _state, _splitter, _timer, _ref}} ->
              {:cont, {:ok, total + length(current)}}

            nil ->
              {:halt, :busy}
          end
        end)

      nil ->
        :busy
    end
  end

  defp batcher_pids!(pipeline_name) do
    batcher_names =
      pipeline_name
      |> Broadway.topology()
      |> Keyword.fetch!(:batchers)
      |> Enum.map(& &1.batcher_name)
      |> Enum.uniq()

    if batcher_names == [] do
      raise "manual batch flushing requires at least one Broadway batcher"
    end

    Enum.map(batcher_names, fn name ->
      GenServer.whereis(name) || raise "Broadway batcher #{inspect(name)} is not running"
    end)
  end

  defp prepare_events!(round, event_count, payload_bytes, producer_pid, queue_tid) do
    payload = String.duplicate("x", payload_bytes)
    ingested_at = DateTime.utc_now()
    offset = round * event_count

    events =
      Enum.map(1..event_count, fn index ->
        id = offset + index

        %LogEvent{
          id: Integer.to_string(id),
          source_id: 1,
          body: %{
            "id" => id,
            "message" => payload,
            "service" => "s3-spool-benchmark",
            "timestamp" => 1_700_000_000_000_000 + id
          },
          event_type: :log,
          ingested_at: ingested_at,
          valid: true,
          drop: false,
          retries: 0,
          day_bucket: 0
        }
      end)

    :ok = IngestEventQueue.add_to_table({{:spool_producer, nil, producer_pid}, queue_tid}, events)
    :erlang.garbage_collect(self())
  end

  defp await_queue!(producer_pid) do
    await_value!(fn -> IngestEventQueue.get_tid({:spool_producer, nil, producer_pid}) end)
  end

  defp await_value!(producer) do
    deadline = System.monotonic_time(:millisecond) + @wait_timeout_ms
    do_await_value!(producer, deadline)
  end

  defp do_await_value!(producer, deadline) do
    case producer.() do
      nil ->
        if System.monotonic_time(:millisecond) >= deadline do
          raise "benchmark timed out waiting for pipeline state"
        else
          receive do
          after
            1 -> do_await_value!(producer, deadline)
          end
        end

      value ->
        value
    end
  end

  defp scheduler_wall_time_snapshot! do
    normal_scheduler_count = System.schedulers_online()

    snapshot =
      :erlang.statistics(:scheduler_wall_time)
      |> Enum.filter(fn {id, _active, _total} -> id <= normal_scheduler_count end)

    if length(snapshot) != normal_scheduler_count do
      raise "expected wall time for #{normal_scheduler_count} normal schedulers, " <>
              "found #{length(snapshot)}"
    end

    Map.new(snapshot, fn {id, active, total} -> {id, {active, total}} end)
  end

  defp scheduler_wall_time_delta!(before, after_snapshot) do
    if Enum.sort(Map.keys(before)) != Enum.sort(Map.keys(after_snapshot)) do
      raise "scheduler wall time snapshots contain different scheduler IDs"
    end

    {active_native, total_native} =
      Enum.reduce(before, {0, 0}, fn {id, {active_before, total_before}},
                                     {active_acc, total_acc} ->
        {active_after, total_after} = Map.fetch!(after_snapshot, id)
        {active_acc + active_after - active_before, total_acc + total_after - total_before}
      end)

    active_us = System.convert_time_unit(active_native, :native, :microsecond)
    total_us = System.convert_time_unit(total_native, :native, :microsecond)

    if active_us <= 0 or total_us <= 0 do
      raise "scheduler wall time deltas must be positive"
    end

    utilization = active_us / total_us

    %{
      active_us: active_us,
      total_us: total_us,
      utilization_percent: utilization * 100,
      average_active_schedulers: utilization * map_size(before)
    }
  end

  defp configure_broadway_handlers!(denominator) do
    initial_handlers = :telemetry.list_handlers([])

    if initial_handlers != [] do
      raise "benchmark requires an empty initial handler table: #{inspect(initial_handlers)}"
    end

    configured_denominator = if denominator == :none, do: :disabled, else: denominator
    Application.put_env(:logflare, :broadway_message_sample_denominator, configured_denominator)

    all_broadway_metrics =
      Logflare.Telemetry.metrics()
      |> Enum.filter(fn metric ->
        Enum.take(metric.event_name, 1) == [:broadway]
      end)

    all_broadway_events = all_broadway_metrics |> Enum.map(& &1.event_name) |> Enum.sort()

    expected_broadway_events =
      if denominator == :none,
        do: List.delete(@expected_broadway_events, @processor_message_event),
        else: @expected_broadway_events

    if all_broadway_events != expected_broadway_events do
      raise "production Broadway metric events differ: #{inspect(all_broadway_events)}"
    end

    broadway_metrics = all_broadway_metrics
    broadway_events = broadway_metrics |> Enum.map(& &1.event_name) |> Enum.sort()

    {:ok, _exporter} =
      OtelMetricExporter.start_link(
        name: @metric_exporter_name,
        metrics: broadway_metrics,
        export_period: :timer.hours(1),
        export_callback: fn _batch, _config -> :ok end,
        resource: %{}
      )

    broadway_handlers = :telemetry.list_handlers([:broadway])
    all_handlers = :telemetry.list_handlers([])

    attached_events = broadway_handlers |> Enum.map(& &1.event_name) |> Enum.sort()

    if attached_events != broadway_events do
      raise "production Broadway handlers differ from metric events: #{inspect(attached_events)}"
    end

    expected_ids =
      MapSet.new(broadway_events, fn event_name ->
        {OtelMetricExporter.TelemetryHandlers, @metric_exporter_name, event_name}
      end)

    actual_ids = MapSet.new(broadway_handlers, & &1.id)

    if actual_ids != expected_ids do
      raise "unexpected production Broadway handler IDs: #{inspect(actual_ids)}"
    end

    %{
      denominator: denominator,
      broadway_total: length(broadway_handlers),
      telemetry_total: length(all_handlers),
      exporter_metrics: length(broadway_metrics),
      exporter_events: length(broadway_events),
      events: broadway_events,
      metric_names: Enum.map(broadway_metrics, &Enum.join(&1.name, ".")) |> Enum.sort()
    }
  end

  defp print_handler_info(info) do
    events = Enum.map_join(info.events, ",", &Enum.join(&1, "."))

    IO.puts(
      "handlers broadway_total=#{info.broadway_total} telemetry_total=#{info.telemetry_total} " <>
        "exporter_metrics=#{info.exporter_metrics} exporter_events=#{info.exporter_events} " <>
        "events=#{events}"
    )
  end

  defp print_metric_store!(info) do
    rows = :ets.tab2list(@metric_exporter_name)

    observations =
      Enum.reduce(rows, %{}, fn
        {{_generation, metric_name, :distribution, _tags, _bucket}, count, _sum}, acc ->
          Map.update(acc, metric_name, count, &(&1 + count))

        row, _acc ->
          raise "unexpected production metric-store row: #{inspect(row)}"
      end)

    actual_metric_names = observations |> Map.keys() |> Enum.sort()

    required_metric_names =
      Enum.reject(info.metric_names, &(&1 == "broadway.processor.message.stop.duration"))

    if Enum.any?(required_metric_names, &(&1 not in actual_metric_names)) or
         actual_metric_names -- info.metric_names != [] do
      raise "metric-store names differ from Broadway metrics: #{inspect(actual_metric_names)}"
    end

    if Enum.any?(observations, fn {_name, count} -> count <= 0 end) do
      raise "every populated Broadway metric must record observations: #{inspect(observations)}"
    end

    observation_total = observations |> Map.values() |> Enum.sum()
    counts = Enum.map_join(actual_metric_names, ",", &"#{&1}:#{Map.fetch!(observations, &1)}")

    IO.puts(
      "metric_store message_sample_denominator=#{format_denominator(info.denominator)} " <>
        "rows=#{length(rows)} metrics=#{length(actual_metric_names)} " <>
        "observations=#{observation_total} message_observations=" <>
        "#{Map.get(observations, "broadway.processor.message.stop.duration", 0)} " <>
        "counts=#{counts}"
    )
  end

  defp format_denominator(:none), do: "none"
  defp format_denominator(denominator), do: Integer.to_string(denominator)

  defp format_sample_rate_percent(:none), do: "0.0"

  defp format_sample_rate_percent(denominator) do
    :erlang.float_to_binary(100 / denominator, decimals: 4)
  end

  defp message_sample_denominator! do
    case System.get_env("MESSAGE_SAMPLE_DENOMINATOR", "none") do
      "none" ->
        :none

      value ->
        denominator = String.to_integer(value)

        if denominator in [1, 10, 100, 1_000, 10_000] do
          denominator
        else
          raise ArgumentError,
                "MESSAGE_SAMPLE_DENOMINATOR must be none, 1, 10, 100, 1000, or 10000"
        end
    end
  end

  defp configure_handler_store!("persistent") do
    :ok = :telemetry.persist()
    verify_handler_backend!(:telemetry_pt)
  end

  defp configure_handler_store!("ets"), do: verify_handler_backend!(:telemetry_ets)

  defp verify_handler_backend!(expected) do
    {actual, _list_for_event, _state} = :persistent_term.get(:telemetry)

    if actual != expected do
      raise "telemetry handler backend must be #{expected}, found #{actual}"
    end

    Atom.to_string(actual)
  end

  defp choice_env!(name, default, choices) do
    value = System.get_env(name, default)

    if value in choices do
      value
    else
      raise "#{name} must be one of #{Enum.join(choices, ", ")}, got #{inspect(value)}"
    end
  end

  defp positive_env!(name, default) do
    case non_negative_env!(name, default) do
      value when value > 0 -> value
      _ -> raise ArgumentError, "#{name} must be greater than zero"
    end
  end

  defp non_negative_env!(name, default) do
    value = System.get_env(name, Integer.to_string(default)) |> String.to_integer()

    if value >= 0 do
      value
    else
      raise ArgumentError, "#{name} must be non-negative"
    end
  end

  defp median(values) do
    sorted = Enum.sort(values)
    count = length(sorted)
    midpoint = div(count, 2)

    if rem(count, 2) == 1 do
      Enum.at(sorted, midpoint)
    else
      (Enum.at(sorted, midpoint - 1) + Enum.at(sorted, midpoint)) / 2
    end
  end

  defp mean(values), do: Enum.sum(values) / length(values)

  defp sample_stdev([_]), do: 0.0

  defp sample_stdev(values) do
    average = mean(values)
    variance = Enum.sum(Enum.map(values, &:math.pow(&1 - average, 2))) / (length(values) - 1)
    :math.sqrt(variance)
  end

  defp round2(value), do: Float.round(value / 1, 2)
end

Logflare.Bench.S3SpoolMetricSampling.run()
