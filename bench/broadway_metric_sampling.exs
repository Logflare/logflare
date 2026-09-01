# Fixed-work Broadway processor-message metric sampling benchmarks.
#
# Scenarios:
#   * noop: identity handle_message/3 with an in-memory producer and acknowledger.
#     This is a deliberately cheap upper bound for relative sampling overhead.
#   * clickhouse: real BufferProducer, IngestEventQueue pointer lifecycle,
#     ClickHouse Pipeline.transform/2, Pipeline.handle_message/3, mapper/RowBinary
#     encoding, gzip batching, and Pipeline.ack/3 cleanup. The network-facing
#     batch callback is replaced by benchmark-local gzip and an in-memory sink.
#
# Run through bench/run_broadway_metric_sampling.sh so every condition gets a
# separate BEAM instance and mirrored execution order.

defmodule Logflare.Bench.BroadwayMetricSampling.Acknowledger do
  @moduledoc false

  @behaviour Broadway.Acknowledger

  @impl true
  def ack({owner, trial_ref, counters, target}, successful, failed) do
    if failed != [] do
      send(owner, {:benchmark_failed, trial_ref, length(failed)})
    end

    :atomics.add(counters, 2, 1)
    completed = :atomics.add_get(counters, 1, length(successful) + length(failed))

    cond do
      completed == target -> send(owner, {:benchmark_complete, trial_ref})
      completed > target -> send(owner, {:benchmark_overflow, trial_ref, completed, target})
      true -> :ok
    end

    :ok
  end
end

defmodule Logflare.Bench.BroadwayMetricSampling.NoopProducer do
  @moduledoc false

  use GenStage

  @behaviour Broadway.Producer

  def prepare(producer, messages) do
    GenStage.call(producer, {__MODULE__, :prepare, messages}, :infinity)
  end

  def release(producer) do
    GenStage.call(producer, {__MODULE__, :release}, :infinity)
  end

  @impl GenStage
  def init(_opts), do: {:producer, %{demand: 0, messages: [], released?: false}}

  @impl GenStage
  def handle_demand(demand, state) do
    state
    |> Map.update!(:demand, &(&1 + demand))
    |> dispatch()
  end

  @impl GenStage
  def handle_call({__MODULE__, :prepare, messages}, _from, %{messages: []} = state) do
    {:reply, :ok, [], %{state | messages: messages, released?: false}}
  end

  def handle_call({__MODULE__, :prepare, _messages}, _from, state) do
    {:reply, {:error, :busy}, [], state}
  end

  def handle_call({__MODULE__, :release}, _from, state) do
    {events, state} = take_demanded(%{state | released?: true})
    {:reply, :ok, events, state}
  end

  defp dispatch(state) do
    {events, state} = take_demanded(state)
    {:noreply, events, state}
  end

  defp take_demanded(%{released?: true, demand: demand, messages: messages} = state)
       when demand > 0 and messages != [] do
    {events, remaining} = Enum.split(messages, demand)

    state = %{
      state
      | demand: demand - length(events),
        messages: remaining,
        released?: remaining != []
    }

    {events, state}
  end

  defp take_demanded(state), do: {[], state}
end

defmodule Logflare.Bench.BroadwayMetricSampling.NoopPipeline do
  @moduledoc false

  use Broadway

  alias Logflare.Bench.BroadwayMetricSampling.NoopProducer

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(opts, :name),
      producer: [module: {NoopProducer, []}, concurrency: 1],
      processors: [
        default: [
          concurrency: Keyword.fetch!(opts, :processor_concurrency),
          min_demand: 100,
          max_demand: 1_000
        ]
      ],
      batchers: [
        default: [
          concurrency: Keyword.fetch!(opts, :batcher_concurrency),
          batch_size: Keyword.fetch!(opts, :batch_size),
          batch_timeout: Keyword.fetch!(opts, :batch_timeout)
        ]
      ]
    )
  end

  @impl Broadway
  def handle_message(_processor, message, _context), do: message

  @impl Broadway
  def handle_batch(:default, messages, _batch_info, _context), do: messages
end

defmodule Logflare.Bench.BroadwayMetricSampling.ClickHouseSink do
  @moduledoc false

  @events 1
  @batches 2
  @bytes 3

  def new, do: :atomics.new(3, signed: false)

  def record(ref, events, bytes) do
    :atomics.add(ref, @events, events)
    :atomics.add(ref, @batches, 1)
    :atomics.add(ref, @bytes, bytes)
    :ok
  end

  def snapshot(ref) do
    %{
      events: :atomics.get(ref, @events),
      batches: :atomics.get(ref, @batches),
      bytes: :atomics.get(ref, @bytes)
    }
  end
end

defmodule Logflare.Bench.BroadwayMetricSampling.ClickHousePipeline do
  @moduledoc false

  use Broadway

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodedRow
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.Bench.BroadwayMetricSampling.ClickHouseSink

  def start_link(opts) do
    backend_id = Keyword.fetch!(opts, :backend_id)

    context =
      backend_id
      |> Pipeline.build_processor_context()
      |> Map.put(:benchmark_sink, Keyword.fetch!(opts, :sink))

    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(opts, :name),
      hibernate_after: 5_000,
      spawn_opt: [fullsweep_after: 100],
      producer: [
        module:
          {BufferProducer,
           [
             backend_id: backend_id,
             consolidated: true,
             id_passing: true,
             interval: 1,
             max_in_flight: Pipeline.max_in_flight(),
             seed_batch_size: Keyword.fetch!(opts, :batch_size)
           ]},
        transformer: {Pipeline, :transform, [backend_id: backend_id]},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: Keyword.fetch!(opts, :processor_concurrency),
          min_demand: 100,
          max_demand: 1_000
        ]
      ],
      batchers: [
        ch: [
          concurrency: Keyword.fetch!(opts, :batcher_concurrency),
          batch_size: Keyword.fetch!(opts, :batch_size),
          batch_timeout: Keyword.fetch!(opts, :batch_timeout)
        ]
      ],
      context: context
    )
  end

  @impl Broadway
  def handle_message(processor_name, message, context) do
    Pipeline.handle_message(processor_name, message, context)
  end

  @impl Broadway
  def handle_batch(
        :ch,
        messages,
        %{batch_key: {event_type, _day_bucket}},
        %{benchmark_sink: sink}
      ) do
    {count, compressed} = compress_rows!(messages, event_type)
    :ok = ClickHouseSink.record(sink, count, byte_size(compressed))
    messages
  end

  # Keep production's complete-batch gzip work while replacing its network insert.
  # RowBinary encoding has already happened in Pipeline.handle_message/3.
  defp compress_rows!(messages, event_type) do
    z = :zlib.open()

    try do
      :zlib.deflateInit(z, :default, :deflated, 31, 8, :default)

      {count, chunks} =
        Enum.reduce(messages, {0, []}, fn
          %{
            data: %EncodedRow{
              pointer: %LogEventPointer{event_type: ^event_type},
              row: row
            }
          },
          {count, chunks} ->
            {count + 1, [:zlib.deflate(z, row) | chunks]}

          message, _acc ->
            raise "unexpected ClickHouse benchmark message: #{inspect(message)}"
        end)

      final_chunk = :zlib.deflate(z, "", :finish)
      {count, IO.iodata_to_binary([Enum.reverse(chunks), final_chunk])}
    after
      :zlib.deflateEnd(z)
      :zlib.close(z)
    end
  end
end

defmodule Logflare.Bench.BroadwayMetricSampling do
  @moduledoc false

  alias Broadway.Message
  alias Broadway.Topology.ProcessorStage
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingConfigStore
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Bench.BroadwayMetricSampling.Acknowledger
  alias Logflare.Bench.BroadwayMetricSampling.ClickHousePipeline
  alias Logflare.Bench.BroadwayMetricSampling.ClickHouseSink
  alias Logflare.Bench.BroadwayMetricSampling.NoopPipeline
  alias Logflare.Bench.BroadwayMetricSampling.NoopProducer

  @pipeline_name Logflare.Bench.BroadwayMetricSamplingPipeline
  @metric_exporter_name Logflare.Bench.BroadwayMetricSamplingMetrics
  @backend_id 1
  @processor_message_event [:broadway, :processor, :message, :stop]
  @expected_broadway_events [
    [:broadway, :batch_processor, :stop],
    [:broadway, :batcher, :stop],
    @processor_message_event,
    [:broadway, :processor, :stop]
  ]
  @wait_timeout_ms 60_000

  @spec run() :: :ok
  def run do
    scenario = choice_env!("SCENARIO", "noop", ["noop", "clickhouse"])
    event_type = event_type!(scenario)
    handler_store = choice_env!("TELEMETRY_HANDLER_STORE", "ets", ["ets", "persistent"])
    message_sample_denominator = message_sample_denominator!()
    events_per_trial = positive_env!("EVENTS", default_events(scenario))
    warmups = non_negative_env!("WARMUPS", 2)
    trials = positive_env!("TRIALS", 5)
    order_index = non_negative_env!("ORDER_INDEX", 0)
    processor_concurrency = positive_env!("PROCESSOR_CONCURRENCY", default_processors(scenario))
    batcher_concurrency = positive_env!("BATCHER_CONCURRENCY", default_batchers(scenario))
    batch_size = positive_env!("BATCH_SIZE", events_per_trial)
    batch_timeout = positive_env!("BATCH_TIMEOUT_MS", 60_000)
    payload_shape = choice_env!("PAYLOAD_SHAPE", "realistic", ["small", "realistic", "large"])

    if rem(events_per_trial, batch_size) != 0 do
      raise ArgumentError, "EVENTS must be divisible by BATCH_SIZE for deterministic full batches"
    end

    if scenario == "clickhouse" do
      Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)
    end

    {:ok, _} = Application.ensure_all_started(:broadway)
    {:module, ProcessorStage} = Code.ensure_loaded(ProcessorStage)
    Logger.configure(level: :error)
    :erlang.system_flag(:scheduler_wall_time, true)
    handler_backend = configure_handler_store!(handler_store)

    compiled_condition =
      if function_exported?(ProcessorStage, :__telemetry_enabled__?, 0),
        do: "telemetry_off",
        else: "telemetry_on"

    if compiled_condition != "telemetry_on" do
      raise "Broadway dependency is compiled with processor telemetry disabled"
    end

    Application.put_env(:logflare, :env, :dev)
    handler_info = configure_broadway_handlers!(message_sample_denominator)

    scenario_state =
      setup_scenario!(
        scenario,
        processor_concurrency,
        batcher_concurrency,
        batch_size,
        batch_timeout
      )

    broadway_version = Application.spec(:broadway, :vsn) |> to_string()
    telemetry_version = Application.spec(:telemetry, :vsn) |> to_string()

    IO.puts(
      "config condition=telemetry_on compiled_variant=#{compiled_condition} " <>
        "scenario=#{scenario} event_type=#{event_type} payload_shape=#{payload_shape} " <>
        "handler_store=#{handler_store} handler_backend=#{handler_backend} " <>
        "message_sample_denominator=#{format_denominator(message_sample_denominator)} " <>
        "configured_message_sample_rate_percent=" <>
        "#{format_sample_rate_percent(message_sample_denominator)} order_index=#{order_index} " <>
        "broadway=#{broadway_version} telemetry=#{telemetry_version} " <>
        "otp=#{System.otp_release()} schedulers=#{System.schedulers_online()} " <>
        "scheduler_wall_time=true lock_counting=#{:erlang.system_info(:lock_counting)} " <>
        "events=#{events_per_trial} warmups=#{warmups} trials=#{trials} " <>
        "processor_concurrency=#{processor_concurrency} " <>
        "batcher_concurrency=#{batcher_concurrency} batch_size=#{batch_size} " <>
        "batch_timeout_ms=#{batch_timeout} output_mode=in_memory"
    )

    print_handler_info(handler_info)

    if warmups > 0 do
      Enum.each(1..warmups, fn index ->
        run_trial(
          :warmup,
          index,
          scenario,
          scenario_state,
          event_type,
          payload_shape,
          events_per_trial
        )
      end)
    end

    results =
      Enum.map(1..trials, fn index ->
        run_trial(
          :measurement,
          index,
          scenario,
          scenario_state,
          event_type,
          payload_shape,
          events_per_trial
        )
      end)

    throughputs = Enum.map(results, & &1.events_per_second)
    reductions_per_event = Enum.map(results, & &1.reductions_per_event)

    IO.puts(
      "summary condition=telemetry_on scenario=#{scenario} event_type=#{event_type} " <>
        "handler_store=#{handler_store} " <>
        "message_sample_denominator=#{format_denominator(message_sample_denominator)} " <>
        "samples=#{trials} median_events_per_second=#{round2(median(throughputs))} " <>
        "mean_events_per_second=#{round2(mean(throughputs))} " <>
        "stdev_events_per_second=#{round2(sample_stdev(throughputs))} " <>
        "median_reductions_per_event=#{round2(median(reductions_per_event))}"
    )

    print_metric_store!(handler_info)
    :ok
  end

  defp setup_scenario!("noop", processor_concurrency, batcher_concurrency, batch_size, timeout) do
    {:ok, _pipeline} =
      NoopPipeline.start_link(
        name: @pipeline_name,
        processor_concurrency: processor_concurrency,
        batcher_concurrency: batcher_concurrency,
        batch_size: batch_size,
        batch_timeout: timeout
      )

    [producer_name] = Broadway.producer_names(@pipeline_name)
    %{producer: producer_name}
  end

  defp setup_scenario!(
         "clickhouse",
         processor_concurrency,
         batcher_concurrency,
         batch_size,
         timeout
       ) do
    {:ok, _registry} = Registry.start_link(keys: :unique, name: BufferProducer.InFlightRegistry)
    {:ok, _queue_manager} = IngestEventQueue.start_link([])
    {:ok, _mapping_config_store} = MappingConfigStore.start_link([])
    sink = ClickHouseSink.new()

    {:ok, _pipeline} =
      ClickHousePipeline.start_link(
        name: @pipeline_name,
        backend_id: @backend_id,
        sink: sink,
        processor_concurrency: processor_concurrency,
        batcher_concurrency: batcher_concurrency,
        batch_size: batch_size,
        batch_timeout: timeout
      )

    [producer_name] = Broadway.producer_names(@pipeline_name)
    producer_pid = GenServer.whereis(producer_name)

    queue_tid =
      await_value!(fn -> IngestEventQueue.get_tid({:consolidated, @backend_id, producer_pid}) end)

    :ok = :sys.suspend(producer_pid)

    %{producer_pid: producer_pid, queue_tid: queue_tid, sink: sink}
  end

  defp run_trial(kind, index, "noop", state, _event_type, _shape, event_count) do
    counters = :atomics.new(2, signed: false)
    trial_ref = make_ref()
    acknowledger = {Acknowledger, {self(), trial_ref, counters, event_count}, :ok}
    messages = Enum.map(1..event_count, &%Message{data: &1, acknowledger: acknowledger})
    :ok = NoopProducer.prepare(state.producer, messages)
    :erlang.garbage_collect(self())

    scheduler_before = scheduler_wall_time_snapshot!()
    {reductions_before, _} = :erlang.statistics(:reductions)
    started_at = System.monotonic_time(:microsecond)

    :ok = NoopProducer.release(state.producer)
    await_noop_complete!(trial_ref, event_count)

    output_events = :atomics.get(counters, 1)
    assert_exact_output_events!("no-op", event_count, output_events)

    finish_trial(
      kind,
      index,
      event_count,
      started_at,
      reductions_before,
      scheduler_before,
      output_events,
      0,
      :atomics.get(counters, 2)
    )
  end

  defp run_trial(kind, index, "clickhouse", state, event_type, shape, event_count) do
    before_sink = ClickHouseSink.snapshot(state.sink)

    prepare_clickhouse_events!(
      event_type,
      shape,
      event_count,
      state.producer_pid,
      state.queue_tid
    )

    target_events = before_sink.events + event_count

    scheduler_before = scheduler_wall_time_snapshot!()
    {reductions_before, _} = :erlang.statistics(:reductions)
    started_at = System.monotonic_time(:microsecond)

    # Remove idle-poll jitter from the timed fixed-work block. The producer's
    # normal handler cancels its tracked timer before scheduling the next one.
    send(state.producer_pid, :scheduled_resolve)
    :ok = :sys.resume(state.producer_pid)
    deadline = System.monotonic_time(:millisecond) + @wait_timeout_ms
    await_clickhouse_complete!(target_events, state, deadline)
    :ok = :sys.suspend(state.producer_pid)

    after_sink = ClickHouseSink.snapshot(state.sink)
    output_events = after_sink.events - before_sink.events
    assert_exact_output_events!("ClickHouse", event_count, output_events)

    finish_trial(
      kind,
      index,
      event_count,
      started_at,
      reductions_before,
      scheduler_before,
      output_events,
      after_sink.bytes - before_sink.bytes,
      after_sink.batches - before_sink.batches
    )
  end

  defp finish_trial(
         kind,
         index,
         event_count,
         started_at,
         reductions_before,
         scheduler_before,
         output_events,
         output_bytes,
         output_batches
       ) do
    elapsed_us = System.monotonic_time(:microsecond) - started_at
    {reductions_after, _} = :erlang.statistics(:reductions)
    scheduler_after = scheduler_wall_time_snapshot!()
    scheduler = scheduler_wall_time_delta!(scheduler_before, scheduler_after)
    reductions = reductions_after - reductions_before
    events_per_second = event_count * 1_000_000 / elapsed_us
    reductions_per_event = reductions / event_count
    events_per_active_scheduler_second = event_count * 1_000_000 / scheduler.active_us

    IO.puts(
      "sample kind=#{kind} run=#{index} events=#{event_count} output_events=#{output_events} " <>
        "elapsed_us=#{elapsed_us} events_per_second=#{round2(events_per_second)} " <>
        "reductions=#{reductions} reductions_per_event=#{round2(reductions_per_event)} " <>
        "output_bytes=#{output_bytes} output_batches=#{output_batches} " <>
        "scheduler_active_us=#{scheduler.active_us} scheduler_total_us=#{scheduler.total_us} " <>
        "scheduler_utilization_percent=#{round2(scheduler.utilization_percent)} " <>
        "average_active_schedulers=#{round2(scheduler.average_active_schedulers)} " <>
        "events_per_active_scheduler_second=#{round2(events_per_active_scheduler_second)}"
    )

    %{events_per_second: events_per_second, reductions_per_event: reductions_per_event}
  end

  defp await_noop_complete!(trial_ref, event_count) do
    receive do
      {:benchmark_complete, ^trial_ref} ->
        :ok

      {:benchmark_failed, ^trial_ref, failed} ->
        raise "no-op benchmark failed #{failed} of #{event_count} messages"

      {:benchmark_overflow, ^trial_ref, completed, ^event_count} ->
        raise "no-op benchmark acknowledged #{completed} of #{event_count} messages"
    after
      @wait_timeout_ms ->
        raise "no-op benchmark timed out waiting for #{event_count} acknowledgements"
    end
  end

  defp prepare_clickhouse_events!(event_type, shape, event_count, producer_pid, queue_tid) do
    events =
      Logflare.Bench.ClickHousePipelineData
      |> apply(:batch, [
        String.to_existing_atom(event_type),
        event_count,
        String.to_existing_atom(shape)
      ])
      |> Enum.map(&%{&1 | day_bucket: 0})

    :ok =
      IngestEventQueue.add_to_table(
        {{:consolidated, @backend_id, producer_pid}, queue_tid},
        events
      )

    :erlang.garbage_collect(self())
  end

  defp await_clickhouse_complete!(target_events, state, deadline) do
    sink = ClickHouseSink.snapshot(state.sink)
    in_flight = BufferProducer.in_flight_count(state.producer_pid)
    queue_size = :ets.info(state.queue_tid, :size)

    generation_size =
      {:consolidated, @backend_id}
      |> IngestEventQueue.list_generations()
      |> Enum.map(fn {tid, _created_at} -> :ets.info(tid, :size) end)
      |> Enum.sum()

    cond do
      sink.events > target_events ->
        raise "ClickHouse benchmark overproduced events: sink_events=#{sink.events} target=#{target_events}"

      sink.events == target_events and in_flight == 0 and queue_size == 0 and
          generation_size == 0 ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        raise "ClickHouse benchmark timed out: sink_events=#{sink.events} target=#{target_events} " <>
                "in_flight=#{in_flight} queue_size=#{queue_size} generation_size=#{generation_size}"

      true ->
        receive do
        after
          1 -> await_clickhouse_complete!(target_events, state, deadline)
        end
    end
  end

  defp assert_exact_output_events!(scenario, expected, actual) do
    if actual != expected do
      raise "#{scenario} benchmark produced #{actual} events, expected exactly #{expected}"
    end

    :ok
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
      |> Enum.filter(fn metric -> Enum.take(metric.event_name, 1) == [:broadway] end)

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

  defp event_type!("noop"), do: "none"

  defp event_type!("clickhouse") do
    choice_env!("EVENT_TYPE", "log", ["log", "metric", "trace"])
  end

  defp default_events("noop"), do: 100_000
  defp default_events("clickhouse"), do: 10_000
  defp default_processors("noop"), do: 1
  defp default_processors("clickhouse"), do: 6
  defp default_batchers("noop"), do: 1
  defp default_batchers("clickhouse"), do: 4

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

Logflare.Bench.BroadwayMetricSampling.run()
