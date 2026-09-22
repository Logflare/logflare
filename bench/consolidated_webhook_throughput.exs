# Fixed-work throughput benchmark: the webhook backend against consolidated_webhook.
#
# This drives the real Broadway topology of both adaptors: BufferProducer, the
# IngestEventQueue path (full events for webhook, pointers for consolidated_webhook),
# JSON encoding, gzip, and ack cleanup. The HTTP call is replaced by an in-memory
# sink that still performs the encode and compress work the Tesla middleware stack
# would, so the result isolates local pipeline throughput rather than receiver
# latency.
#
# Reported as events per second. Each design moves the same number of events.
#
# SOURCES is the dimension that matters. The webhook backend runs one Broadway
# topology per source (3 processors and 6 batchers each), while consolidated_webhook
# runs one topology per backend no matter how many sources feed it. A single source
# therefore favours the webhook backend, and the consolidated design is built for the
# many-source case.
#
# Usage:
#
#   mix run --no-start bench/consolidated_webhook_throughput.exs
#   SOURCES=1,8,32 EVENTS=192000 mix run --no-start bench/consolidated_webhook_throughput.exs
#   BATCH_SIZE=5000 TRIALS=7 WARMUPS=3 mix run --no-start bench/consolidated_webhook_throughput.exs
#
# EVENTS must divide evenly by both batch sizes and by every source count, so no trial
# waits on a partial batch timeout.

Code.require_file("support/clickhouse_pipeline_bench_data.exs", __DIR__)
Code.compiler_options(ignore_module_conflict: true)

defmodule Logflare.Bench.WebhookThroughputSink do
  @moduledoc false

  @batches_key {__MODULE__, :batches}
  @latency_key {__MODULE__, :latency_ms}

  @spec setup(non_neg_integer()) :: :ok
  def setup(latency_ms) do
    :persistent_term.put(@batches_key, :atomics.new(1, signed: false))
    :persistent_term.put(@latency_key, latency_ms)
    :ok
  end

  @doc """
  Stands in for the receiver's round trip.

  With no latency the benchmark is purely CPU bound, which only rewards whichever
  design runs more worker processes. A real receiver makes the request itself the
  bottleneck, and that is what makes batch size matter: one design sends 250 events
  per request, the other sends `batch_size`.
  """
  @spec await_receiver() :: :ok
  def await_receiver do
    case :persistent_term.get(@latency_key) do
      0 -> :ok
      ms -> Process.sleep(ms)
    end
  end

  @spec reset() :: :ok
  def reset, do: :atomics.put(ref(), 1, 0)

  @spec batches_sent() :: non_neg_integer()
  def batches_sent, do: :atomics.get(ref(), 1)

  @spec record_sent() :: :ok
  def record_sent, do: :atomics.add(ref(), 1, 1)

  defp ref, do: :persistent_term.get(@batches_key)
end

# Replaces the real HTTP client for the duration of the benchmark. Tesla's JSON and
# CompressRequest middleware run inside Client.send/1, so the sink repeats that work
# before discarding the payload. Without it the comparison would omit the encode and
# compress cost that both designs actually pay.
defmodule Logflare.Backends.Adaptor.WebhookAdaptor.Client do
  @moduledoc false

  alias Logflare.Bench.WebhookThroughputSink

  @spec send(keyword()) :: {:ok, Tesla.Env.t()}
  def send(opts) do
    payload =
      case Keyword.fetch!(opts, :body) do
        body when is_binary(body) -> body
        bodies -> Jason.encode!(bodies)
      end

    if Keyword.get(opts, :gzip, true), do: :zlib.gzip(payload), else: payload
    WebhookThroughputSink.await_receiver()
    WebhookThroughputSink.record_sent()

    {:ok, %Tesla.Env{status: 204}}
  end
end

defmodule Logflare.Bench.ConsolidatedWebhookThroughput do
  @moduledoc false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Bench.ClickHousePipelineData
  alias Logflare.Bench.WebhookThroughputSink
  alias Logflare.ContextCache
  alias Logflare.Sources.Source

  @handle_batch_event [:logflare, :backends, :pipeline, :handle_batch]
  @events_key {__MODULE__, :events}
  @drain_timeout_ms 120_000
  @webhook_backend_id 9_300_001
  @consolidated_backend_id 9_300_002
  @source_id 9_300_101

  @spec run() :: :ok
  def run do
    total_events = positive_env!("EVENTS", 96_000)
    warmups = non_negative_env!("WARMUPS", 1)
    trials = positive_env!("TRIALS", 3)
    batch_size = positive_env!("BATCH_SIZE", 1_000)
    source_counts = list_env!("SOURCES", "1,4,16")
    latency_ms = non_negative_env!("SINK_LATENCY_MS", 20)

    boot(batch_size, Enum.max(source_counts), latency_ms)
    check_divisible!(total_events, batch_size, source_counts)

    events = ClickHousePipelineData.batch(:log, total_events, :realistic)
    bytes = events |> Enum.map(&:erlang.external_size(&1.body)) |> Enum.sum()

    IO.puts(
      "config events=#{total_events} warmups=#{warmups} trials=#{trials} " <>
        "webhook_batch_size=250 consolidated_batch_size=#{batch_size} " <>
        "sink_latency_ms=#{latency_ms} " <>
        "bytes_per_event=#{div(bytes, total_events)} " <>
        "schedulers=#{System.schedulers_online()} otp=#{System.otp_release()} sink=in_memory"
    )

    IO.puts("")

    header()

    for sources <- source_counts do
      rates =
        for design <- [:webhook, :consolidated_webhook], into: %{} do
          if warmups > 0, do: Enum.each(1..warmups, fn _ -> trial(design, events, sources) end)
          samples = Enum.map(1..trials, fn _ -> trial(design, events, sources) end)
          {design, summarize(total_events, samples)}
        end

      row(sources, rates)
    end

    :ok
  end

  @spec trial(atom(), [Logflare.LogEvent.t()], pos_integer()) :: pos_integer()
  defp trial(design, events, sources) do
    pids = start_design!(design, sources)
    Enum.each(queues_keys(design, sources), &await_queue!/1)
    # add_to_table/3 falls back to the startup queue whenever no producer queue has
    # room. That table is created by the supervision tree in production; without it
    # here the fallback returns :not_initialized and the events are dropped.
    Enum.each(queues_keys(design, sources), &startup_queue!/1)
    reset_counters()

    insert(design, events, sources)
    await_drain!(length(events))
    finished_at = System.monotonic_time(:microsecond)
    elapsed = finished_at - first_batch_at()

    Enum.each(pids, &stop_design!/1)
    Enum.each(queues_keys(design, sources), &await_no_producer!/1)
    Enum.each(queues_keys(design, sources), &clear_queues/1)
    elapsed
  end

  # The webhook pipeline is per source; consolidated_webhook is one per backend. Each
  # design is started fresh per trial so a trial never inherits the previous queue.
  @spec start_design!(atom(), pos_integer()) :: [pid()]
  defp start_design!(:webhook, sources) do
    for index <- 1..sources do
      {:ok, pid} = WebhookAdaptor.start_link({source(index), webhook_backend()})
      pid
    end
  end

  defp start_design!(:consolidated_webhook, _sources) do
    {:ok, pid} = ConsolidatedWebhookAdaptor.start_link(consolidated_backend())
    [pid]
  end

  # Waiting on the supervisor pid alone is not enough. Broadway traps exits, so the
  # topology can still hold its registered name when the next trial starts, which fails
  # with :already_started. The producer queue disappearing is the signal that the whole
  # topology is down.
  @spec stop_design!(pid()) :: :ok
  defp stop_design!(pid) do
    ref = Process.monitor(pid)
    Process.unlink(pid)
    Process.exit(pid, :shutdown)

    receive do
      {:DOWN, ^ref, :process, ^pid, _reason} -> :ok
    after
      5_000 -> raise "pipeline did not stop"
    end
  end

  @spec await_no_producer!(tuple()) :: :ok
  defp await_no_producer!(key) do
    Enum.reduce_while(1..500, nil, fn _, _ ->
      case Enum.reject(IngestEventQueue.list_queues(key), &match?({_, _, nil}, &1)) do
        [] -> {:halt, :ok}
        [_ | _] -> {:cont, Process.sleep(10)}
      end
    end)
    |> case do
      :ok -> :ok
      _ -> raise "producer for #{inspect(key)} did not shut down"
    end
  end

  # Every source contributes the same share of the run, so both designs move an
  # identical event total whatever the source count is.
  @spec insert(atom(), [Logflare.LogEvent.t()], pos_integer()) :: :ok
  defp insert(design, events, sources) do
    per_source = div(length(events), sources)

    events
    |> Enum.chunk_every(per_source)
    |> Enum.zip(queues_keys(design, sources) |> Stream.cycle() |> Enum.take(sources))
    |> Enum.each(fn {chunk, key} -> IngestEventQueue.add_to_table(key, chunk) end)
  end

  @spec startup_queue!(tuple()) :: :ok
  defp startup_queue!({owner, backend_id}), do: ensure_queue!({owner, backend_id, nil})

  defp ensure_queue!(key) do
    case IngestEventQueue.upsert_tid(key) do
      {:ok, _tid} -> :ok
      {:error, :already_exists, _tid} -> :ok
    end
  end

  @spec queues_keys(atom(), pos_integer()) :: [tuple()]
  defp queues_keys(:webhook, sources),
    do: for(index <- 1..sources, do: {@source_id + index, @webhook_backend_id})

  defp queues_keys(:consolidated_webhook, _sources),
    do: [{:consolidated, @consolidated_backend_id}]

  # A producer registers its own queue on init. Inserting before that lands the batch
  # in the startup queue instead, which no producer of this design ever drains.
  @spec await_queue!(tuple()) :: :ok
  defp await_queue!(key) do
    Enum.reduce_while(1..500, nil, fn _, _ ->
      case Enum.reject(IngestEventQueue.list_queues(key), &match?({_, _, nil}, &1)) do
        [] -> {:cont, Process.sleep(10)}
        [_ | _] -> {:halt, :ok}
      end
    end)
    |> case do
      :ok -> :ok
      _ -> raise "no producer queue appeared for #{inspect(key)}"
    end
  end

  # Completion needs both counters. The handle_batch telemetry fires when a batch
  # reaches the batcher, while the sink counts a batch only once its encode and gzip
  # finish, so waiting on the pair means no trial stops mid-batch.
  @spec await_drain!(pos_integer()) :: :ok
  defp await_drain!(total_events) do
    deadline = System.monotonic_time(:millisecond) + @drain_timeout_ms

    Enum.reduce_while(Stream.cycle([:tick]), nil, fn _, _ ->
      {events_seen, batches_seen} = counters()

      cond do
        events_seen >= total_events and WebhookThroughputSink.batches_sent() >= batches_seen ->
          {:halt, :ok}

        System.monotonic_time(:millisecond) > deadline ->
          {:halt, {:timeout, events_seen}}

        true ->
          {:cont, Process.sleep(1)}
      end
    end)
    |> case do
      :ok -> :ok
      {:timeout, seen} -> raise "drain timed out after #{seen}/#{total_events} events"
    end
  end

  @spec clear_queues(tuple()) :: :ok
  defp clear_queues(key) do
    key
    |> IngestEventQueue.list_queues()
    |> Enum.each(&IngestEventQueue.delete_queue/1)
  end

  @spec summarize(pos_integer(), [pos_integer()]) :: map()
  defp summarize(total_events, samples) do
    sorted = samples |> Enum.map(&(total_events * 1_000_000 / &1)) |> Enum.sort()

    %{
      median: Enum.at(sorted, div(length(sorted), 2)),
      min: List.first(sorted),
      max: List.last(sorted)
    }
  end

  @spec header() :: :ok
  defp header do
    IO.puts("sources | webhook ev/s | consolidated_webhook ev/s | ratio | webhook topologies")

    IO.puts(String.duplicate("-", 78))
  end

  @spec row(pos_integer(), map()) :: :ok
  defp row(sources, %{webhook: webhook, consolidated_webhook: consolidated}) do
    IO.puts(
      "#{String.pad_leading(to_string(sources), 7)} | " <>
        "#{fmt(webhook.median)} | " <>
        "#{fmt(consolidated.median)}              | " <>
        "#{String.pad_leading(Float.to_string(Float.round(consolidated.median / webhook.median, 2)), 5)} | " <>
        "#{sources} (#{sources * 9} workers) vs 1 (6 workers)"
    )
  end

  defp fmt(rate), do: rate |> round() |> Integer.to_string() |> String.pad_leading(8)

  @spec boot(pos_integer(), pos_integer(), non_neg_integer()) :: :ok
  defp boot(batch_size, max_sources, latency_ms) do
    {:ok, _} = Application.ensure_all_started(:broadway)
    {:ok, _} = Application.ensure_all_started(:cachex)
    Logger.configure(level: String.to_existing_atom(System.get_env("LOG_LEVEL", "error")))
    Application.put_env(:logflare, :env, :test)

    WebhookThroughputSink.setup(latency_ms)
    :persistent_term.put(@events_key, :atomics.new(3, signed: true))

    children = [
      {Registry, name: Backends.SourceRegistry, keys: :unique},
      {Registry, name: Backends.BackendRegistry, keys: :unique},
      {Registry, name: BufferProducer.InFlightRegistry, keys: :unique},
      %{id: :sources_cache, start: {Cachex, :start_link, [Logflare.Sources.Cache, []]}},
      %{id: :backends_cache, start: {Cachex, :start_link, [Logflare.Backends.Cache, []]}},
      %{id: :rates_cache, start: {Cachex, :start_link, [Logflare.PubSubRates.Cache, []]}},
      IngestEventQueue
    ]

    {:ok, _} = Supervisor.start_link(children, strategy: :one_for_one)

    seed_caches(batch_size, max_sources)
    attach_telemetry()
  end

  # Both pipelines read the backend through the context cache in handle_batch, and the
  # webhook producer reads its source the same way. Seeding the cache directly keeps
  # the benchmark free of a database.
  @spec seed_caches(pos_integer(), pos_integer()) :: :ok
  defp seed_caches(batch_size, max_sources) do
    for index <- 1..max_sources do
      put_cached(Logflare.Sources, {:get_by, [[id: @source_id + index]]}, source(index))
    end

    put_cached(Backends, {:get_backend, [@webhook_backend_id]}, webhook_backend())

    put_cached(
      Backends,
      {:get_backend, [@consolidated_backend_id]},
      consolidated_backend(batch_size)
    )

    :ok
  end

  # ContextCache.fetch/3 wraps every value in a :cached tuple so a nil result still
  # counts as a hit. Writing the same shape here means the lookup never reaches the
  # repo, which is what keeps this benchmark database free.
  @spec put_cached(module(), tuple(), term()) :: :ok
  defp put_cached(context, cache_key, value) do
    {:ok, true} = Cachex.put(ContextCache.cache_name(context), cache_key, {:cached, value})
    :ok
  end

  @spec attach_telemetry() :: :ok
  defp attach_telemetry do
    :telemetry.attach(
      "#{__MODULE__}-handle-batch",
      @handle_batch_event,
      fn _event, %{batch_size: size}, _metadata, _config ->
        ref = :persistent_term.get(@events_key)
        :atomics.add(ref, 1, size)
        :atomics.add(ref, 2, 1)
        # compare_exchange keeps the first writer, so this is the start of steady state
        :atomics.compare_exchange(ref, 3, 0, System.monotonic_time(:microsecond))
      end,
      nil
    )
  end

  defp counters do
    ref = :persistent_term.get(@events_key)
    {:atomics.get(ref, 1), :atomics.get(ref, 2)}
  end

  # A producer that finds an empty queue sleeps until its next poll, and that idle wait
  # is not throughput. Timing from the first dispatched batch measures the sustained
  # rate of each design instead of its start-up latency.
  defp first_batch_at, do: :atomics.get(:persistent_term.get(@events_key), 3)

  defp reset_counters do
    ref = :persistent_term.get(@events_key)
    :atomics.put(ref, 1, 0)
    :atomics.put(ref, 2, 0)
    :atomics.put(ref, 3, 0)
    WebhookThroughputSink.reset()
  end

  defp source(index) do
    id = @source_id + index

    %Source{
      id: id,
      token: :"00000000-0000-0000-0000-#{String.pad_leading(to_string(id), 12, "0")}",
      name: "bench source #{index}",
      user_id: 1
    }
  end

  defp webhook_backend do
    %Backend{
      id: @webhook_backend_id,
      type: :webhook,
      name: "bench webhook",
      token: Ecto.UUID.generate(),
      user_id: 1,
      config: %{url: "https://bench.invalid", http: "http2", gzip: true, format: "json"}
    }
  end

  defp consolidated_backend(batch_size \\ 1_000) do
    %Backend{
      id: @consolidated_backend_id,
      type: :consolidated_webhook,
      name: "bench consolidated webhook",
      token: Ecto.UUID.generate(),
      user_id: 1,
      config: %{
        url: "https://bench.invalid",
        http: "http2",
        gzip: true,
        format: "json",
        batch_size: batch_size
      },
      config_encrypted: %{
        url: "https://bench.invalid",
        http: "http2",
        gzip: true,
        format: "json",
        batch_size: batch_size
      }
    }
  end

  defp check_divisible!(events, batch_size, source_counts) do
    for sources <- source_counts do
      if rem(events, sources) != 0 do
        raise "EVENTS=#{events} must divide evenly by SOURCES=#{sources}"
      end

      for size <- [250, batch_size], rem(div(events, sources), size) != 0 do
        raise "EVENTS/#{sources} must divide evenly by #{size} so no trial waits on a partial batch"
      end
    end

    :ok
  end

  defp list_env!(name, default) do
    System.get_env(name, default)
    |> String.split(",", trim: true)
    |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
    |> Enum.sort()
  end

  defp positive_env!(name, default) do
    value = System.get_env(name, to_string(default)) |> String.to_integer()
    if value > 0, do: value, else: raise("#{name} must be positive")
  end

  defp non_negative_env!(name, default) do
    value = System.get_env(name, to_string(default)) |> String.to_integer()
    if value >= 0, do: value, else: raise("#{name} must not be negative")
  end
end

Logflare.Bench.ConsolidatedWebhookThroughput.run()
