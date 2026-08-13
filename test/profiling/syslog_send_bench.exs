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

alias Logflare.Backends.Adaptor.SyslogAdaptor.Socket
alias Logflare.Backends.Adaptor.SyslogAdaptor.Syslog
alias SyslogBenchSupport, as: Support

Support.ensure_apps_started()
Support.print_config()

batch_size = Support.batch_size()
event_count = Support.events()

inputs =
  Map.new(Support.message_bytes(), fn bytes ->
    {"#{bytes} byte message", Support.build_events(event_count, bytes)}
  end)

setup = fn events ->
  sink = Support.start_sink()
  {:ok, socket} = Socket.connect(Support.config(sink.port), 5_000)

  %{events: events, sink: sink, socket: socket}
end

teardown = fn state ->
  :ok = Socket.close(state.socket)
  :ok = Support.stop_sink(state.sink)
  state
end

send_and_wait = fn send_fun ->
  fn state ->
    ref = Support.expect_frames(state.sink, length(state.events))
    :ok = send_fun.(state.events, state.socket)
    Support.await_frames(ref)
  end
end

config = %{max_message_bytes: 50_000}

# credo:disable-for-next-line Credo.Check.Refactor.IoPuts
IO.puts("Each iteration sends #{event_count} events; multiply iterations/second by that count.")

Benchee.run(
  %{
    "handle_message routes, handle_batch formats+sends batch" =>
      send_and_wait.(fn events, socket ->
        Enum.each(Enum.chunk_every(events, batch_size), fn events ->
          content = for event <- events, do: Syslog.format(event, config)
          :ok = Socket.send(socket, content)
        end)

        :ok
      end),
    "no batcher: handle_message formats+sends each message" =>
      send_and_wait.(fn events, socket ->
        Enum.each(events, fn event ->
          :ok = Socket.send(socket, Syslog.format(event, config))
        end)

        :ok
      end),
    "preformat in handle_message, handle_batch sends preformatted frames" =>
      send_and_wait.(fn events, socket ->
        events
        |> Enum.map(&Syslog.format(&1, config))
        |> Enum.chunk_every(batch_size)
        |> Enum.each(fn frames ->
          :ok = Socket.send(socket, frames)
        end)

        :ok
      end)
  },
  inputs: inputs,
  before_scenario: setup,
  after_scenario: teardown,
  time: Support.time(),
  warmup: Support.warmup()
)
