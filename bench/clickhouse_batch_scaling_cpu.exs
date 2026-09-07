# CPU and correctness benchmark for ClickHouse's exact-key scaling path.
#
# The queue/encoding benchmark does not exercise the work added by batch-aware
# scaling: scanning startup pointer metadata, filtered startup claims, or the
# least-loaded routing sort. This benchmark covers those operations directly.
#
# Usage:
#
#   mix run --no-start bench/clickhouse_batch_scaling_cpu.exs
#   BENCH_SECTION=scaling BENCH_TIME=5 BENCH_WARMUP=2 \
#     mix run --no-start bench/clickhouse_batch_scaling_cpu.exs
#
# BENCH_SECTION may be all, scaling, claims, or routing.

defmodule Logflare.Bench.ClickHouseBatchScaling do
  @moduledoc false

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.LogEvent

  @batch_size 60_000
  @routing_rows 100
  @routing_queues 8

  @type input :: map()

  @spec setup_scaling_inputs() :: %{String.t() => input()}
  def setup_scaling_inputs do
    [
      {"60k/one-complete-key", 31_000_001, 60_000, [{:fresh, :log, 20_000}], 2},
      {"60k/three-partial-keys", 31_000_002, 60_000,
       [{:fresh, :log, 20_000}, {:fresh, :metric, 20_000}, {:stale, :log, 19_999}], 1},
      {"120k/two-complete-keys", 31_000_003, 120_000,
       [{:fresh, :log, 20_000}, {:fresh, :metric, 20_000}], 2}
    ]
    |> Map.new(fn {label, backend_id, rows, keys, expected_desired} ->
      input = setup_scaling_input(backend_id, rows, keys, expected_desired)
      validate_scaling!(input)
      {label, input}
    end)
  end

  @spec setup_routing_inputs() :: %{String.t() => input()}
  def setup_routing_inputs do
    ensure_queue_started()

    [
      {"consolidated/eight-balanced-queues", :consolidated},
      {"standard/eight-balanced-queues", :standard},
      {"spool/eight-balanced-queues", :spool}
    ]
    |> Map.new(fn {label, kind} ->
      {queues_key, startup_key, queue_key_fun} = routing_keys(kind)
      upsert_queue(startup_key)

      queue_keys =
        Enum.map(1..@routing_queues, fn _index ->
          pid = spawn(fn -> Process.sleep(:infinity) end)
          key = queue_key_fun.(pid)
          upsert_queue(key)
          key
        end)

      events =
        Enum.map(1..@routing_rows, fn index ->
          event("route-#{kind}-#{index}", {:fresh, :log, 20_000})
        end)

      input = %{queues_key: queues_key, queue_keys: queue_keys, events: events}
      validate_routing!(input)
      {label, input}
    end)
  end

  @spec scale_candidate?(input()) :: boolean()
  def scale_candidate?(%{queues_key: queues_key, startup_key: startup_key}) do
    IngestEventQueue.list_pending_counts(queues_key)
    |> Enum.find_value(false, fn
      {^startup_key, count} -> count >= @batch_size
      _entry -> false
    end)
  end

  @spec exact_key_scale_decision(input()) :: non_neg_integer()
  def exact_key_scale_decision(%{queues_key: queues_key, startup_key: startup_key}) do
    lens = IngestEventQueue.list_pending_counts(queues_key)

    counts =
      if startup_size(lens, startup_key) >= Pipeline.max_batch_size() do
        IngestEventQueue.pending_batch_key_counts(startup_key)
      else
        %{}
      end

    ClickHouseAdaptor.resolve_pipeline_count(
      %{pipeline_count: 1, max_pipelines: 8, last_count_decrease: nil},
      lens,
      counts
    )
  end

  @spec pending_batch_key_counts(input()) :: map()
  def pending_batch_key_counts(%{startup_key: startup_key}) do
    IngestEventQueue.pending_batch_key_counts(startup_key)
  end

  @spec claim_and_reinsert(input(), :filtered | :unfiltered) :: non_neg_integer()
  def claim_and_reinsert(
        %{startup_key: startup_key, claim_key: claim_key, claim_size: claim_size},
        mode
      ) do
    result =
      case mode do
        :filtered ->
          IngestEventQueue.pop_pending_pointers_by_batch_key(
            startup_key,
            claim_key,
            claim_size
          )

        :unfiltered ->
          IngestEventQueue.pop_pending_pointers(startup_key, claim_size)
      end

    {:ok, pointers, _tid} = result
    Enum.each(pointers, &IngestEventQueue.reinsert_pointer/1)
    length(pointers)
  end

  @spec route_and_drain(input(), :direct | :least_loaded) :: non_neg_integer()
  def route_and_drain(
        %{queues_key: queues_key, queue_keys: [direct_key | _] = queue_keys, events: events},
        mode
      ) do
    case mode do
      :direct -> :ok = IngestEventQueue.add_to_table(direct_key, events)
      :least_loaded -> :ok = IngestEventQueue.add_to_table(queues_key, events)
    end

    pointers =
      Enum.flat_map(queue_keys, fn key ->
        {:ok, claimed, _tid} = IngestEventQueue.pop_pending_pointers(key, length(events))
        claimed
      end)

    if length(pointers) != length(events) do
      raise "routing benchmark claimed #{length(pointers)} of #{length(events)} pointers"
    end

    Enum.each(pointers, fn pointer ->
      IngestEventQueue.delete_id(pointer.tid, pointer.gen_event_id)
    end)

    length(pointers)
  end

  defp setup_scaling_input(backend_id, rows, keys, expected_desired) do
    ensure_queue_started()
    startup_key = {:consolidated, backend_id, nil}
    upsert_queue(startup_key)

    1..rows
    |> Stream.chunk_every(1_000)
    |> Enum.each(fn indexes ->
      events =
        Enum.map(indexes, fn index ->
          key = Enum.at(keys, rem(index - 1, length(keys)))
          event("#{backend_id}-#{index}", key)
        end)

      :ok = IngestEventQueue.add_to_table(startup_key, events)
    end)

    expected_counts =
      1..rows
      |> Enum.map(fn index -> Enum.at(keys, rem(index - 1, length(keys))) end)
      |> Enum.frequencies()

    {claim_key, claim_size} = Enum.max_by(expected_counts, &elem(&1, 1))

    %{
      backend_id: backend_id,
      queues_key: {:consolidated, backend_id},
      startup_key: startup_key,
      rows: rows,
      expected_counts: expected_counts,
      expected_desired: expected_desired,
      claim_key: claim_key,
      claim_size: min(claim_size, @batch_size)
    }
  end

  defp validate_scaling!(input) do
    actual_counts = pending_batch_key_counts(input)

    if actual_counts != input.expected_counts do
      raise "batch-key count validation failed: #{inspect(actual_counts)}"
    end

    if exact_key_scale_decision(input) != input.expected_desired do
      raise "scale decision validation failed for backend #{input.backend_id}"
    end

    for mode <- [:filtered, :unfiltered] do
      claimed = claim_and_reinsert(input, mode)

      if claimed != input.claim_size or pending_batch_key_counts(input) != input.expected_counts do
        raise "#{mode} claim validation failed for backend #{input.backend_id}"
      end
    end

    gen_tid = IngestEventQueue.current_generation_tid(input.queues_key)

    if IngestEventQueue.total_pending(input.startup_key) != input.rows or
         :ets.info(gen_tid, :size) != input.rows do
      raise "queue preservation validation failed for backend #{input.backend_id}"
    end

    :ok
  end

  defp validate_routing!(input) do
    for mode <- [:direct, :least_loaded] do
      if route_and_drain(input, mode) != length(input.events) do
        raise "#{mode} routing validation failed"
      end

      if Enum.any?(input.queue_keys, &(IngestEventQueue.total_pending(&1) != 0)) do
        raise "#{mode} routing validation left pending pointers"
      end

      gen_tid = IngestEventQueue.current_generation_tid(input.queues_key)

      if :ets.info(gen_tid, :size) != 0 do
        raise "#{mode} routing validation left generation rows"
      end
    end

    :ok
  end

  defp routing_keys(:consolidated) do
    backend_id = 31_000_010

    {
      {:consolidated, backend_id},
      {:consolidated, backend_id, nil},
      &{:consolidated, backend_id, &1}
    }
  end

  defp routing_keys(:standard) do
    source_id = 31_000_011
    backend_id = 31_000_012
    {{source_id, backend_id}, {source_id, backend_id, nil}, &{source_id, backend_id, &1}}
  end

  defp routing_keys(:spool) do
    {{:spool_producer, nil}, {:spool_producer, nil, nil}, &{:spool_producer, nil, &1}}
  end

  defp startup_size(lens, startup_key) do
    Enum.find_value(lens, 0, fn
      {^startup_key, count} -> count
      _entry -> false
    end)
  end

  defp event(id, {freshness, event_type, day_bucket}) do
    %LogEvent{
      id: id,
      body: %{"value" => id},
      retries: 0,
      event_type: event_type,
      day_bucket: day_bucket,
      ingest_freshness: freshness
    }
  end

  defp ensure_queue_started do
    case Process.whereis(IngestEventQueue) do
      nil ->
        {:ok, _pid} = IngestEventQueue.start_link([])
        :ok

      _pid ->
        :ok
    end
  end

  defp upsert_queue(key) do
    case IngestEventQueue.upsert_tid(key) do
      {:ok, _tid} -> :ok
      {:error, :already_exists, _tid} -> :ok
    end
  end
end

alias Logflare.Bench.ClickHouseBatchScaling, as: Scaling

section = System.get_env("BENCH_SECTION", "all")
time = System.get_env("BENCH_TIME", "3") |> String.to_integer()
warmup = System.get_env("BENCH_WARMUP", "1") |> String.to_integer()
memory_time = System.get_env("BENCH_MEMORY_TIME", "1") |> String.to_integer()
reduction_time = System.get_env("BENCH_REDUCTION_TIME", "1") |> String.to_integer()

run_benchmark = fn title, scenarios, inputs ->
  IO.puts("\n== #{title} ==\n")

  Benchee.run(scenarios,
    inputs: inputs,
    time: time,
    warmup: warmup,
    memory_time: memory_time,
    reduction_time: reduction_time,
    print: [configuration: false]
  )
end

if section in ["all", "scaling", "claims"] do
  scaling_inputs = Scaling.setup_scaling_inputs()

  if section in ["all", "scaling"] do
    run_benchmark.(
      "10-second scaler tick",
      %{
        "aggregate candidate check" => &Scaling.scale_candidate?/1,
        "exact-key scale decision" => &Scaling.exact_key_scale_decision/1,
        "exact-key metadata scan only" => &Scaling.pending_batch_key_counts/1
      },
      scaling_inputs
    )
  end

  if section in ["all", "claims"] do
    claim_inputs =
      Map.drop(scaling_inputs, ["60k/three-partial-keys"])

    run_benchmark.(
      "startup pointer claim and reinsert",
      %{
        "filtered exact-key claim" => &Scaling.claim_and_reinsert(&1, :filtered),
        "unfiltered claim" => &Scaling.claim_and_reinsert(&1, :unfiltered)
      },
      claim_inputs
    )
  end
end

if section in ["all", "routing"] do
  routing_inputs = Scaling.setup_routing_inputs()

  run_benchmark.(
    "100-row routing and drain across eight producer queues",
    %{
      "direct queue" => &Scaling.route_and_drain(&1, :direct),
      "least-loaded routing" => &Scaling.route_and_drain(&1, :least_loaded)
    },
    routing_inputs
  )
end
