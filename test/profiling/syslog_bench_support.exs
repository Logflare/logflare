defmodule SyslogBenchSupport do
  @moduledoc false

  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Sources.Source

  @default_events 5_000
  @default_batch_size 50
  @default_concurrency 5
  @default_push_chunk 1_000
  @default_message_bytes [200, 2_000, 50_000]
  @default_time 5
  @default_warmup 2

  def events, do: env_int("SYSLOG_BENCH_EVENTS", @default_events)
  def batch_size, do: env_int("SYSLOG_BENCH_BATCH_SIZE", @default_batch_size)
  def concurrency, do: env_int("SYSLOG_BENCH_CONCURRENCY", @default_concurrency)
  def push_chunk, do: env_int("SYSLOG_BENCH_PUSH_CHUNK", @default_push_chunk)
  def message_bytes, do: env_int_list("SYSLOG_BENCH_MESSAGE_BYTES", @default_message_bytes)
  def time, do: env_number("SYSLOG_BENCH_TIME", @default_time)
  def warmup, do: env_number("SYSLOG_BENCH_WARMUP", @default_warmup)

  def print_config(extra \\ []) do
    config =
      [
        events: events(),
        batch_size: batch_size(),
        concurrency: concurrency(),
        push_chunk: push_chunk(),
        message_bytes: Enum.join(message_bytes(), ","),
        time: time(),
        warmup: warmup()
      ] ++ extra

    IO.puts("Syslog benchmark config: #{inspect(config)}")
  end

  def ensure_apps_started do
    for app <- [:logger, :crypto, :ssl, :telemetry, :cachex, :nimble_pool, :gen_stage, :broadway] do
      case Application.ensure_all_started(app) do
        {:ok, _apps} -> :ok
        {:error, _reason} -> :ok
      end
    end
  end

  def ensure_cache(name) do
    case Process.whereis(name) do
      nil -> Cachex.start_link(name, [])
      _pid -> {:ok, name}
    end
  end

  def ensure_ingest_queue_started do
    case Process.whereis(Logflare.Backends.IngestEventQueue) do
      nil -> Logflare.Backends.IngestEventQueue.start_link([])
      pid -> {:ok, pid}
    end
  end

  def cache_bench_source_and_backend(source, backend) do
    Cachex.put(Logflare.Sources.Cache, {:get_by, [[id: source.id]]}, {:cached, source})
    Cachex.put(Logflare.Sources.Cache, {:get_by, [[token: source.token]]}, {:cached, source})
    Cachex.put(Logflare.Backends.Cache, {:get_backend, [backend.id]}, {:cached, backend})
  end

  def source do
    %Source{
      id: System.unique_integer([:positive]),
      name: "syslog-bench",
      token: :"syslog-bench-#{System.unique_integer([:positive])}",
      metrics: %{avg: 0},
      rules: []
    }
  end

  def backend(port) do
    %Backend{
      id: System.unique_integer([:positive]),
      name: "syslog-bench",
      type: :syslog,
      config: %{
        host: "127.0.0.1",
        port: port
      }
    }
  end

  def config(port) do
    %{
      host: "127.0.0.1",
      port: port
    }
  end

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
end

defmodule SyslogBenchSupport.TcpSink do
  @moduledoc false

  def start_link(expected_frames) do
    parent = self()
    ref = make_ref()

    pid =
      spawn_link(fn ->
        {:ok, listen_socket} =
          :gen_tcp.listen(0, [
            :binary,
            packet: :raw,
            active: false,
            reuseaddr: true,
            ip: {127, 0, 0, 1}
          ])

        {:ok, {_ip, port}} = :inet.sockname(listen_socket)
        send(parent, {:syslog_sink_ready, ref, self(), port})
        accept_loop(listen_socket, parent, ref)
      end)

    receive do
      {:syslog_sink_ready, ^ref, ^pid, port} ->
        state = %{pid: pid, ref: ref, port: port, expected_frames: expected_frames}
        {:ok, state}
    after
      5_000 -> raise "timed out starting syslog sink"
    end
  end

  def port(%{port: port}), do: port
  def ref(%{ref: ref}), do: ref

  def stop(%{pid: pid}) do
    Process.exit(pid, :normal)
    :ok
  end

  defp accept_loop(listen_socket, parent, ref) do
    case :gen_tcp.accept(listen_socket, 250) do
      {:ok, socket} ->
        spawn_link(fn -> recv_loop(socket, parent, ref, <<>>, 0, 0) end)
        accept_loop(listen_socket, parent, ref)

      {:error, :timeout} ->
        accept_loop(listen_socket, parent, ref)

      {:error, reason} ->
        send(parent, {:syslog_sink_error, ref, reason})
    end
  end

  defp recv_loop(socket, parent, ref, buffer, frames, bytes) do
    case :gen_tcp.recv(socket, 0, 5_000) do
      {:ok, data} ->
        {buffer, new_frames, new_bytes} = parse_frames(buffer <> data, frames, bytes)
        send(parent, {:syslog_sink_frames, ref, new_frames - frames, new_bytes - bytes})
        recv_loop(socket, parent, ref, buffer, new_frames, new_bytes)

      {:error, :closed} ->
        :ok

      {:error, reason} ->
        send(parent, {:syslog_sink_error, ref, reason})
    end
  end

  defp parse_frames(buffer, frames, bytes) do
    case parse_frame(buffer) do
      {:ok, rest, frame_bytes} ->
        parse_frames(rest, frames + 1, bytes + frame_bytes)

      :more ->
        {buffer, frames, bytes}
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

defmodule SyslogBenchSupport.SinkCollector do
  @moduledoc false

  def collect(%{ref: ref, expected_frames: expected_frames}) do
    collect(ref, expected_frames, 0, 0)
  end

  defp collect(_ref, expected_frames, frames, bytes) when frames >= expected_frames do
    %{frames: frames, bytes: bytes}
  end

  defp collect(ref, expected_frames, frames, bytes) do
    receive do
      {:syslog_sink_frames, ^ref, frame_count, byte_count} ->
        collect(ref, expected_frames, frames + frame_count, bytes + byte_count)

      {:syslog_sink_error, ^ref, reason} ->
        raise "syslog sink failed: #{inspect(reason)}"
    after
      30_000 -> raise "timed out after receiving #{frames}/#{expected_frames} syslog frames"
    end
  end
end

defmodule SyslogBenchSupport.NimbleTcpPool do
  @moduledoc false

  @behaviour NimblePool

  def start_link(opts) do
    config = Keyword.fetch!(opts, :config)

    NimblePool.start_link(
      worker: {__MODULE__, config},
      lazy: true,
      name: Keyword.get(opts, :name)
    )
  end

  def send(pool, iodata) do
    NimblePool.checkout!(pool, :checkout, fn _from, socket ->
      case :gen_tcp.send(socket, iodata) do
        :ok -> {:ok, socket}
        {:error, reason} = error -> {error, {:remove, reason}}
      end
    end)
  end

  @impl NimblePool
  def init_pool(config), do: {:ok, config}

  @impl NimblePool
  def init_worker(config) do
    host = config |> Map.fetch!(:host) |> String.to_charlist()
    port = Map.fetch!(config, :port)

    opts = [:binary, packet: :raw, active: false, nodelay: true]
    {:ok, socket} = :gen_tcp.connect(host, port, opts, 5_000)
    {:ok, socket, config}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, socket, config) do
    {:ok, socket, socket, config}
  end

  @impl NimblePool
  def handle_checkin(socket, _from, _old_socket, config) do
    {:ok, socket, config}
  end

  @impl NimblePool
  def handle_info(_message, socket), do: {:ok, socket}

  @impl NimblePool
  def terminate_worker(_reason, socket, config) do
    :gen_tcp.close(socket)
    {:ok, config}
  end
end
