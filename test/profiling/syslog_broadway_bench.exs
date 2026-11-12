# Benchmark syslog adaptor pipeline shapes end-to-end through Broadway.
#
# Run with:
#   mix run --no-start test/profiling/syslog_broadway_bench.exs
#
# Useful env:
#   SYSLOG_BENCH_MESSAGE_BYTES=200,2000,50000
#   SYSLOG_BENCH_CONCURRENCY=1,5  # processor concurrency
#   SYSLOG_BENCH_BATCHER_CONCURRENCY=5
#   SYSLOG_BENCH_BATCH_SIZE=50
#   SYSLOG_BENCH_EVENTS=5000
#   SYSLOG_BENCH_TIME=5
#   SYSLOG_BENCH_WARMUP=2

Code.require_file("syslog_bench_support.exs", __DIR__)

defmodule SyslogBroadwayBench do
  use Broadway

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Pipeline
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Pool
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
  alias SyslogBenchSupport, as: Support

  @spec setup(map(), :batched | :unbatched | :preformatted) :: map()
  def setup(input, mode) do
    sink = Support.start_sink()
    source = Support.source()
    backend = Support.backend(sink.port)
    :ok = Support.cache_backend(backend)

    pool_name = __MODULE__.Pool
    pipeline_name = __MODULE__.Pipeline

    {:ok, pool} =
      Pool.start_link(
        backend_id: backend.id,
        name: pool_name,
        worker_idle_timeout: 60_000
      )

    context = %{
      backend_id: backend.id,
      mode: mode,
      pool: pool,
      source_id: source.id
    }

    broadway_opts = [
      pipeline_module: __MODULE__,
      producer: [module: {Broadway.DummyProducer, []}],
      processors: [default: [concurrency: input.concurrency, min_demand: 1]],
      batchers: batchers(mode),
      context: context
    ]

    {:ok, pipeline} =
      Pipeline.start_link(
        [source: source, backend: backend, pool: pool, name: pipeline_name],
        broadway_opts
      )

    state = %{
      events: Enum.map(input.events, &Pipeline.transform(&1, [])),
      pipeline: pipeline,
      pipeline_name: pipeline_name,
      pool: pool,
      sink: sink
    }

    run(state)
    state
  end

  @spec run(map()) :: map()
  def run(state) do
    ref = Support.expect_frames(state.sink, length(state.events))
    :ok = Broadway.push_messages(state.pipeline_name, state.events)
    Support.await_frames(ref)
  end

  @spec teardown(map()) :: map()
  def teardown(state) do
    :ok = Broadway.stop(state.pipeline)
    :ok = GenServer.stop(state.pool)
    :ok = Support.stop_sink(state.sink)
    state
  end

  @impl Broadway
  def handle_message(_processor_name, message, %{mode: :batched}) do
    Message.put_batcher(message, :syslog)
  end

  def handle_message(_processor_name, message, %{mode: :unbatched} = context) do
    config = lookup_backend_config(context.backend_id)
    send_one(message, Syslog.format(message.data, config), context.pool)
  end

  def handle_message(_processor_name, message, %{mode: :preformatted} = context) do
    config = lookup_backend_config(context.backend_id)

    message
    |> Message.update_data(&Syslog.format(&1, config))
    |> Message.put_batcher(:syslog)
  end

  @impl Broadway
  def handle_batch(:syslog, messages, _batch_info, %{mode: :batched} = context) do
    config = lookup_backend_config(context.backend_id)
    content = for %Message{data: event} <- messages, do: Syslog.format(event, config)
    send_batch(messages, content, context.pool)
  end

  def handle_batch(:syslog, messages, _batch_info, %{mode: :preformatted} = context) do
    content = for %Message{data: frame} <- messages, do: frame
    send_batch(messages, content, context.pool)
  end

  defp batchers(:unbatched), do: []

  defp batchers(_mode) do
    [
      syslog: [
        concurrency: Support.batcher_concurrency(),
        batch_size: Support.batch_size()
      ]
    ]
  end

  defp lookup_backend_config(backend_id) do
    %{config: config} =
      Backends.Cache.get_backend(backend_id) || raise "missing backend #{backend_id}"

    if cipher_key = config[:cipher_key] do
      Map.put(config, :cipher_key, Base.decode64!(cipher_key))
    else
      config
    end
  end

  defp send_one(message, content, pool) do
    case Pool.send(pool, content) do
      :ok -> message
      {:error, reason} -> Message.failed(message, reason)
    end
  end

  defp send_batch(messages, content, pool) do
    case Pool.send(pool, content) do
      :ok -> messages
      {:error, reason} -> Enum.map(messages, &Message.failed(&1, reason))
    end
  end
end

alias SyslogBenchSupport, as: Support

Support.ensure_apps_started()
Support.ensure_cache(Logflare.Backends.Cache)
Support.print_config()

event_count = Support.events()

inputs =
  Map.new(
    for bytes <- Support.message_bytes(), concurrency <- Support.processor_concurrency() do
      label = "#{bytes} byte message, processor concurrency #{concurrency}"
      {label, %{concurrency: concurrency, events: Support.build_events(event_count, bytes)}}
    end
  )

job = fn mode ->
  {&SyslogBroadwayBench.run/1,
   before_scenario: &SyslogBroadwayBench.setup(&1, mode),
   after_scenario: &SyslogBroadwayBench.teardown/1}
end

# credo:disable-for-next-line Credo.Check.Refactor.IoPuts
IO.puts("Each iteration sends #{event_count} events; multiply iterations/second by that count.")

Benchee.run(
  %{
    "broadway: handle_message routes, handle_batch formats+sends batch" => job.(:batched),
    "broadway: no batcher, handle_message formats+sends each message" => job.(:unbatched),
    "broadway: handle_message preformats, handle_batch sends preformatted frames" =>
      job.(:preformatted)
  },
  inputs: inputs,
  time: Support.time(),
  warmup: Support.warmup()
)
