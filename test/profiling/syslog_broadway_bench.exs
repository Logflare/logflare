# Benchmark syslog adaptor pipeline shapes end-to-end through Broadway.
#
# Run with:
#   mix run --no-start test/profiling/syslog_broadway_bench.exs
#
# Useful env:
#   SYSLOG_BENCH_MESSAGE_BYTES=200,2000,50000
#   SYSLOG_BENCH_CONCURRENCY=1,5
#   SYSLOG_BENCH_BATCH_SIZE=50
#   SYSLOG_BENCH_EVENTS=5000
#   SYSLOG_BENCH_PUSH_CHUNK=1000
#   SYSLOG_BENCH_TIME=5
#   SYSLOG_BENCH_WARMUP=2

Code.require_file("syslog_bench_support.exs", __DIR__)

defmodule SyslogBroadwayBench.Pipeline do
  @moduledoc false

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
  alias SyslogBenchSupport.NimbleTcpPool

  def producer(source, backend) do
    [
      module:
        {Backends.BufferProducer,
         backend_id: backend.id,
         source_id: source.id,
         interval: 10,
         buffer_size: SyslogBenchSupport.events() * 2},
      transformer: {__MODULE__, :transform, []}
    ]
  end

  def processor_opts(concurrency) do
    [default: [concurrency: concurrency, min_demand: 1]]
  end

  def batcher_opts(concurrency, batch_size) do
    [syslog: [concurrency: concurrency, batch_size: batch_size]]
  end

  def context(pool, backend) do
    %{pool: pool, backend_id: backend.id}
  end

  def send_batch(pool, content, messages) do
    case NimbleTcpPool.send(pool, content) do
      :ok -> messages
      {:error, reason} -> fail_batch(messages, reason)
    end
  end

  def lookup_backend_config(backend_id) do
    %{config: config} =
      Backends.Cache.get_backend(backend_id) || raise "missing backend #{backend_id}"

    config
  end

  def transform(event, _opts) do
    %Message{data: event, acknowledger: {__MODULE__, _ref = nil, _meta = []}}
  end

  def ack(_ack_ref, _successful, _failed), do: :ok

  defp fail_batch(messages, reason) do
    Enum.map(messages, fn message ->
      Broadway.Message.failed(message, reason)
    end)
  end
end

defmodule SyslogBroadwayBench.BatchedPipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
  alias SyslogBroadwayBench.Pipeline

  @behaviour Broadway.Acknowledger

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(opts, :name),
      producer: Pipeline.producer(Keyword.fetch!(opts, :source), Keyword.fetch!(opts, :backend)),
      processors: Pipeline.processor_opts(Keyword.fetch!(opts, :processor_concurrency)),
      batchers:
        Pipeline.batcher_opts(
          Keyword.fetch!(opts, :batcher_concurrency),
          Keyword.fetch!(opts, :batch_size)
        ),
      context: Pipeline.context(Keyword.fetch!(opts, :pool), Keyword.fetch!(opts, :backend))
    )
  end

  @impl Broadway
  def handle_message(_processor_name, message, _context) do
    Message.put_batcher(message, :syslog)
  end

  @impl Broadway
  def handle_batch(:syslog, messages, _batch_info, context) do
    config = Pipeline.lookup_backend_config(context.backend_id)
    content = for %Message{data: event} <- messages, do: Syslog.format(event, config)
    Pipeline.send_batch(context.pool, content, messages)
  end

  @impl Broadway.Acknowledger
  defdelegate ack(ack_ref, successful, failed), to: Pipeline
end

defmodule SyslogBroadwayBench.NoBatcherPipeline do
  @moduledoc false

  use Broadway

  alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
  alias SyslogBenchSupport.NimbleTcpPool
  alias SyslogBroadwayBench.Pipeline

  @behaviour Broadway.Acknowledger

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(opts, :name),
      producer: Pipeline.producer(Keyword.fetch!(opts, :source), Keyword.fetch!(opts, :backend)),
      processors: Pipeline.processor_opts(Keyword.fetch!(opts, :processor_concurrency)),
      context: Pipeline.context(Keyword.fetch!(opts, :pool), Keyword.fetch!(opts, :backend))
    )
  end

  @impl Broadway
  def handle_message(_processor_name, message, context) do
    config = Pipeline.lookup_backend_config(context.backend_id)

    case NimbleTcpPool.send(context.pool, Syslog.format(message.data, config)) do
      :ok -> message
      {:error, reason} -> Broadway.Message.failed(message, reason)
    end
  end

  @impl Broadway.Acknowledger
  defdelegate ack(ack_ref, successful, failed), to: Pipeline
end

defmodule SyslogBroadwayBench.PreformattedPipeline do
  @moduledoc false

  use Broadway

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
  alias SyslogBroadwayBench.Pipeline

  @behaviour Broadway.Acknowledger

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: Keyword.fetch!(opts, :name),
      producer: Pipeline.producer(Keyword.fetch!(opts, :source), Keyword.fetch!(opts, :backend)),
      processors: Pipeline.processor_opts(Keyword.fetch!(opts, :processor_concurrency)),
      batchers:
        Pipeline.batcher_opts(
          Keyword.fetch!(opts, :batcher_concurrency),
          Keyword.fetch!(opts, :batch_size)
        ),
      context: Pipeline.context(Keyword.fetch!(opts, :pool), Keyword.fetch!(opts, :backend))
    )
  end

  @impl Broadway
  def handle_message(_processor_name, message, context) do
    config = Pipeline.lookup_backend_config(context.backend_id)

    message
    |> Message.update_data(fn event -> Syslog.format(event, config) end)
    |> Message.put_batcher(:syslog)
  end

  @impl Broadway
  def handle_batch(:syslog, messages, _batch_info, context) do
    content = for %Message{data: frame} <- messages, do: frame
    Pipeline.send_batch(context.pool, content, messages)
  end

  @impl Broadway.Acknowledger
  defdelegate ack(ack_ref, successful, failed), to: Pipeline
end

alias Logflare.Backends.IngestEventQueue
alias SyslogBenchSupport.NimbleTcpPool
alias SyslogBenchSupport.SinkCollector
alias SyslogBenchSupport.TcpSink

SyslogBenchSupport.ensure_apps_started()
SyslogBenchSupport.ensure_cache(Logflare.Sources.Cache)
SyslogBenchSupport.ensure_cache(Logflare.Backends.Cache)
SyslogBenchSupport.ensure_cache(Logflare.PubSubRates.Cache)
SyslogBenchSupport.ensure_ingest_queue_started()
SyslogBenchSupport.print_config()

event_count = SyslogBenchSupport.events()
batch_size = SyslogBenchSupport.batch_size()
concurrency = SyslogBenchSupport.concurrency()
push_chunk = SyslogBenchSupport.push_chunk()

inputs =
  Map.new(SyslogBenchSupport.message_bytes(), fn bytes ->
    {"#{bytes} byte message", SyslogBenchSupport.build_events(event_count, bytes)}
  end)

run_pipeline = fn pipeline_module ->
  fn events ->
    {:ok, sink} = TcpSink.start_link(length(events))

    source = SyslogBenchSupport.source()
    backend = sink |> TcpSink.port() |> SyslogBenchSupport.backend()
    SyslogBenchSupport.cache_bench_source_and_backend(source, backend)

    {:ok, pool} = NimbleTcpPool.start_link(config: SyslogBenchSupport.config(TcpSink.port(sink)))

    {:ok, pipeline} =
      pipeline_module.start_link(
        name: :"#{inspect(pipeline_module)}-#{System.unique_integer([:positive])}",
        source: source,
        backend: backend,
        pool: pool,
        processor_concurrency: concurrency,
        batcher_concurrency: concurrency,
        batch_size: batch_size
      )

    events
    |> Enum.chunk_every(push_chunk)
    |> Enum.each(fn events ->
      :ok = IngestEventQueue.add_to_table({source.id, backend.id}, events, chunk_size: push_chunk)
    end)

    stats = SinkCollector.collect(sink)

    Broadway.stop(pipeline)
    GenServer.stop(pool)
    TcpSink.stop(sink)

    stats
  end
end

Benchee.run(
  %{
    "broadway: handle_message routes, handle_batch formats+sends batch" =>
      run_pipeline.(SyslogBroadwayBench.BatchedPipeline),
    "broadway: no batcher, handle_message formats+sends each message" =>
      run_pipeline.(SyslogBroadwayBench.NoBatcherPipeline),
    "broadway: handle_message preformats, handle_batch sends preformatted frames" =>
      run_pipeline.(SyslogBroadwayBench.PreformattedPipeline)
  },
  inputs: inputs,
  time: SyslogBenchSupport.time(),
  warmup: SyslogBenchSupport.warmup(),
  memory_time: 1,
  reduction_time: 1
)
