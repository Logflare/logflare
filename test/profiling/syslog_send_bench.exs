# Benchmark syslog formatter + TCP send shapes against a local RFC6587 sink.
#
# Run with:
#   mix run --no-start test/profiling/syslog_send_bench.exs
#
# Useful env:
#   SYSLOG_BENCH_MESSAGE_BYTES=200,2000,50000
#   SYSLOG_BENCH_BATCH_SIZE=50
#   SYSLOG_BENCH_EVENTS=5000
#   SYSLOG_BENCH_TIME=5
#   SYSLOG_BENCH_WARMUP=2

Code.require_file("syslog_bench_support.exs", __DIR__)

alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
alias SyslogBenchSupport.SinkCollector
alias SyslogBenchSupport.TcpSink

SyslogBenchSupport.ensure_apps_started()
SyslogBenchSupport.print_config()

batch_size = SyslogBenchSupport.batch_size()
event_count = SyslogBenchSupport.events()
message_bytes = SyslogBenchSupport.message_bytes()

inputs =
  Map.new(message_bytes, fn bytes ->
    {"#{bytes} byte message", SyslogBenchSupport.build_events(event_count, bytes)}
  end)

connect = fn port ->
  opts = [
    :binary,
    packet: :raw,
    active: false,
    nodelay: true
  ]

  Enum.reduce_while(1..20, nil, fn _, _ ->
    case :gen_tcp.connect(~c"127.0.0.1", port, opts, 1_000) do
      {:ok, socket} ->
        {:halt, socket}

      {:error, reason} when reason in [:econnrefused, :etimedout] ->
        Process.sleep(10)
        {:cont, nil}

      {:error, reason} ->
        raise "failed to connect to syslog sink: #{inspect(reason)}"
    end
  end) || raise "timed out connecting to syslog sink"
end

send_and_wait = fn send_fun ->
  fn events ->
    {:ok, sink} = TcpSink.start_link(length(events))
    socket = connect.(TcpSink.port(sink))

    send_fun.(events, socket)

    :gen_tcp.close(socket)
    stats = SinkCollector.collect(sink)
    TcpSink.stop(sink)
    stats
  end
end

config = %{}

Benchee.run(
  %{
    "handle_message routes, handle_batch formats+sends batch" =>
      send_and_wait.(fn events, socket ->
        events
        |> Enum.chunk_every(batch_size)
        |> Enum.each(fn events ->
          content = for event <- events, do: Syslog.format(event, config)
          :ok = :gen_tcp.send(socket, content)
        end)
      end),
    "no batcher: handle_message formats+sends each message" =>
      send_and_wait.(fn events, socket ->
        Enum.each(events, fn event ->
          :ok = :gen_tcp.send(socket, Syslog.format(event, config))
        end)
      end),
    "preformat in handle_message, handle_batch sends preformatted frames" =>
      send_and_wait.(fn events, socket ->
        events
        |> Enum.map(&Syslog.format(&1, config))
        |> Enum.chunk_every(batch_size)
        |> Enum.each(fn frames ->
          :ok = :gen_tcp.send(socket, frames)
        end)
      end)
  },
  inputs: inputs,
  time: SyslogBenchSupport.time(),
  warmup: SyslogBenchSupport.warmup(),
  memory_time: 1,
  reduction_time: 1
)
