defmodule SyslogBenchSupport do
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Sources.Source

  @default_batch_size 50
  @default_batcher_concurrency 5
  @default_processor_concurrency [1, 5]
  @default_events 5_000
  @default_message_bytes [200, 2_000, 50_000]
  @default_time 5
  @default_warmup 2

  @spec batch_size() :: pos_integer()
  def batch_size, do: env_int("SYSLOG_BENCH_BATCH_SIZE", @default_batch_size)

  @spec batcher_concurrency() :: pos_integer()
  def batcher_concurrency do
    env_int("SYSLOG_BENCH_BATCHER_CONCURRENCY", @default_batcher_concurrency)
  end

  @spec processor_concurrency() :: [pos_integer()]
  def processor_concurrency do
    env_int_list("SYSLOG_BENCH_CONCURRENCY", @default_processor_concurrency)
  end

  @spec events() :: pos_integer()
  def events, do: env_int("SYSLOG_BENCH_EVENTS", @default_events)

  @spec message_bytes() :: [pos_integer()]
  def message_bytes, do: env_int_list("SYSLOG_BENCH_MESSAGE_BYTES", @default_message_bytes)

  @spec time() :: number()
  def time, do: env_number("SYSLOG_BENCH_TIME", @default_time)

  @spec warmup() :: number()
  def warmup, do: env_number("SYSLOG_BENCH_WARMUP", @default_warmup)

  @spec print_config() :: :ok
  def print_config do
    # credo:disable-for-next-line Credo.Check.Refactor.IoPuts
    IO.puts(
      "Syslog benchmark config: " <>
        inspect(
          events: events(),
          batch_size: batch_size(),
          batcher_concurrency: batcher_concurrency(),
          processor_concurrency: Enum.join(processor_concurrency(), ","),
          message_bytes: Enum.join(message_bytes(), ","),
          time: time(),
          warmup: warmup()
        )
    )
  end

  @spec ensure_apps_started() :: :ok
  def ensure_apps_started do
    for app <- [:logger, :crypto, :ssl, :telemetry, :cachex, :nimble_pool, :gen_stage, :broadway] do
      {:ok, _apps} = Application.ensure_all_started(app)
    end

    :ok
  end

  @spec ensure_cache(atom()) :: :ok
  def ensure_cache(name) do
    case Process.whereis(name) do
      nil ->
        {:ok, _pid} = Cachex.start_link(name, [])
        :ok

      _pid ->
        :ok
    end
  end

  @spec cache_backend(Backend.t()) :: :ok
  def cache_backend(backend) do
    {:ok, true} =
      Cachex.put(Logflare.Backends.Cache, {:get_backend, [backend.id]}, {:cached, backend})

    :ok
  end

  @spec source() :: Source.t()
  def source do
    %Source{
      id: System.unique_integer([:positive]),
      name: "syslog-bench",
      token: :"syslog-bench",
      metrics: %{avg: 0},
      rules: []
    }
  end

  @spec backend(:inet.port_number()) :: Backend.t()
  def backend(port) do
    %Backend{
      id: System.unique_integer([:positive]),
      name: "syslog-bench",
      type: :syslog,
      config: config(port)
    }
  end

  @spec config(:inet.port_number()) :: map()
  def config(port) do
    %{
      host: "127.0.0.1",
      max_message_bytes: 50_000,
      port: port
    }
  end

  @spec build_events(pos_integer(), pos_integer()) :: [LogEvent.t()]
  def build_events(count, message_bytes) do
    message = String.duplicate("x", message_bytes)
    now = System.system_time(:microsecond)

    for i <- 1..count do
      %LogEvent{
        id: Ecto.UUID.generate(),
        body: %{
          "timestamp" => now + i,
          "event_message" => message,
          "metadata" => %{
            "level" => "info",
            "app_name" => "syslog-bench",
            "procid" => "bench"
          }
        },
        event_type: :log,
        valid: true
      }
    end
  end

  @spec start_sink() :: %{pid: pid(), port: :inet.port_number()}
  def start_sink do
    parent = self()
    pid = spawn(fn -> init_sink(parent) end)

    receive do
      {:syslog_sink_ready, ^pid, port} -> %{pid: pid, port: port}
    after
      5_000 -> raise "timed out starting syslog sink"
    end
  end

  @spec expect_frames(map(), pos_integer()) :: reference()
  def expect_frames(%{pid: pid}, expected_frames) do
    ref = make_ref()
    send(pid, {:expect_frames, self(), ref, expected_frames})

    receive do
      {:syslog_sink_armed, ^ref} -> ref
      {:syslog_sink_busy, ^ref} -> raise "syslog sink already has an active expectation"
    after
      5_000 -> raise "timed out arming syslog sink"
    end
  end

  @spec await_frames(reference(), timeout()) :: %{bytes: non_neg_integer(), frames: pos_integer()}
  def await_frames(ref, timeout \\ 30_000) do
    receive do
      {:syslog_sink_done, ^ref, stats} -> stats
      {:syslog_sink_error, reason} -> raise "syslog sink failed: #{inspect(reason)}"
    after
      timeout -> raise "timed out waiting for syslog frames"
    end
  end

  @spec stop_sink(map()) :: :ok
  def stop_sink(%{pid: pid}) do
    monitor = Process.monitor(pid)
    send(pid, :stop)

    receive do
      {:DOWN, ^monitor, :process, ^pid, _reason} -> :ok
    after
      5_000 -> raise "timed out stopping syslog sink"
    end
  end

  defp env_int(name, default) do
    case System.get_env(name) do
      nil -> default
      value -> String.to_integer(value)
    end
  end

  defp env_int_list(name, default) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        value
        |> String.split(",", trim: true)
        |> Enum.map(&String.to_integer/1)
    end
  end

  defp env_number(name, default) do
    case System.get_env(name) do
      nil ->
        default

      value ->
        {number, ""} = Float.parse(value)
        number
    end
  end

  defp init_sink(parent) do
    sink = self()
    acceptor = spawn(fn -> accept_connections(sink) end)

    receive do
      {:syslog_sink_listening, ^acceptor, port} ->
        send(parent, {:syslog_sink_ready, sink, port})

        sink_loop(%{
          acceptor: acceptor,
          bytes: 0,
          frames: 0,
          expectation: nil
        })
    after
      5_000 -> raise "timed out opening syslog listen socket"
    end
  end

  defp sink_loop(state) do
    receive do
      {:expect_frames, caller, ref, expected_frames} when state.expectation == nil ->
        send(caller, {:syslog_sink_armed, ref})

        sink_loop(%{
          state
          | expectation: %{
              caller: caller,
              ref: ref,
              start_bytes: state.bytes,
              start_frames: state.frames,
              target: state.frames + expected_frames
            }
        })

      {:expect_frames, caller, ref, _expected_frames} ->
        send(caller, {:syslog_sink_busy, ref})
        sink_loop(state)

      {:syslog_frames, frame_count, byte_count} ->
        state
        |> Map.update!(:frames, &(&1 + frame_count))
        |> Map.update!(:bytes, &(&1 + byte_count))
        |> maybe_complete_expectation()
        |> sink_loop()

      {:syslog_sink_error, reason} ->
        if state.expectation do
          send(state.expectation.caller, {:syslog_sink_error, reason})
        end

        sink_loop(state)

      :stop ->
        Process.exit(state.acceptor, :shutdown)
        :ok
    end
  end

  defp maybe_complete_expectation(%{expectation: nil} = state), do: state

  defp maybe_complete_expectation(%{frames: frames, expectation: %{target: target}} = state)
       when frames >= target do
    expectation = state.expectation

    send(
      expectation.caller,
      {:syslog_sink_done, expectation.ref,
       %{
         bytes: state.bytes - expectation.start_bytes,
         frames: state.frames - expectation.start_frames
       }}
    )

    %{state | expectation: nil}
  end

  defp maybe_complete_expectation(state), do: state

  defp accept_connections(sink) do
    {:ok, listen_socket} =
      :gen_tcp.listen(0, [
        :binary,
        packet: :raw,
        active: false,
        reuseaddr: true,
        ip: {127, 0, 0, 1}
      ])

    {:ok, {_ip, port}} = :inet.sockname(listen_socket)
    send(sink, {:syslog_sink_listening, self(), port})
    accept_connection(listen_socket, sink)
  end

  defp accept_connection(listen_socket, sink) do
    {:ok, socket} = :gen_tcp.accept(listen_socket)
    receiver = spawn(fn -> receive_frames(sink) end)
    :ok = :gen_tcp.controlling_process(socket, receiver)
    send(receiver, {:socket, socket})
    accept_connection(listen_socket, sink)
  end

  defp receive_frames(sink) do
    receive do
      {:socket, socket} -> receive_frames(socket, sink, <<>>)
    end
  end

  defp receive_frames(socket, sink, buffer) do
    case :gen_tcp.recv(socket, 0, :infinity) do
      {:ok, data} ->
        {buffer, frames, bytes} = parse_frames(buffer <> data, 0, 0)
        send(sink, {:syslog_frames, frames, bytes})
        receive_frames(socket, sink, buffer)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        send(sink, {:syslog_sink_error, reason})
    end
  end

  defp parse_frames(buffer, frames, bytes) do
    case parse_frame(buffer) do
      {:ok, rest, frame_bytes} -> parse_frames(rest, frames + 1, bytes + frame_bytes)
      :more -> {buffer, frames, bytes}
    end
  end

  defp parse_frame(buffer) do
    case :binary.match(buffer, " ") do
      {idx, 1} ->
        <<len_bin::binary-size(idx), " ", rest::binary>> = buffer
        len = String.to_integer(len_bin)

        if byte_size(rest) >= len do
          <<_frame::binary-size(len), tail::binary>> = rest
          {:ok, tail, idx + 1 + len}
        else
          :more
        end

      :nomatch ->
        :more
    end
  end
end
