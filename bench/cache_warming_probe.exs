# Manual benchmark probe for proactive cache warming.
# See BENCHMARK-proactive-cache-warming.md for the full procedure.
#
# Inside a running `make start` IEx session:
#
#   c "bench/cache_warming_probe.exs"
#   CacheWarmingProbe.start(label: "baseline")
#   # ... run load ...
#   CacheWarmingProbe.stop()
#
# Every window (default 10s) prints and appends to /tmp/cache_warming_<label>.csv:
#
#   t_s        seconds since start
#   reqs       ingest requests completed in the window
#   p50/p99/max_ms  ingest request latency (router dispatch: auth, source lookup, controller)
#   slow       requests slower than :slow_ms (default 100ms)
#   errors     requests with status >= 400 or raised exceptions
#   rp_selects SELECTs not issued in the background, i.e. read-through cache misses
#              (plus any background noise, see the quiet baseline)
#   bg_selects / bg_ms  SELECTs issued by cache warmers or refresh-ahead tasks, and their total DB time
#   tables     rp_selects broken down by table
defmodule CacheWarmingProbe do
  @moduledoc false

  @table :cache_warming_probe
  @handler "cache-warming-probe"
  @reporter :cache_warming_probe_reporter
  @ingest_routes ["/logs", "/api/logs", "/api/events"]
  @events [
    [:phoenix, :router_dispatch, :stop],
    [:phoenix, :router_dispatch, :exception],
    [:logflare, :repo, :query]
  ]
  @csv_header "t_s,reqs,p50_ms,p99_ms,max_ms,slow,errors,rp_selects,bg_selects,bg_ms,tables\n"

  @spec start(keyword()) :: :ok | {:error, :timeout}
  def start(opts \\ []) do
    stop()

    label = Keyword.get(opts, :label, "run")

    config = %{
      window_ms: Keyword.get(opts, :window_ms, 10_000),
      slow_ms: Keyword.get(opts, :slow_ms, 100),
      csv: Keyword.get(opts, :csv, "/tmp/cache_warming_#{label}.csv"),
      started_at: System.monotonic_time(:millisecond)
    }

    parent = self()
    pid = spawn(fn -> init(config, parent) end)

    receive do
      {:ready, ^pid} ->
        IO.puts("probe started, writing #{config.csv}")
        IO.puts(format_header())
        :ok
    after
      5_000 -> {:error, :timeout}
    end
  end

  @spec stop() :: :ok
  def stop do
    :telemetry.detach(@handler)

    case Process.whereis(@reporter) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        send(pid, :stop)

        receive do
          {:DOWN, ^ref, _, _, _} -> :ok
        after
          5_000 -> :ok
        end
    end
  end

  def handle_event([:phoenix, :router_dispatch, :stop], %{duration: d}, meta, _) do
    if meta.route in @ingest_routes, do: insert({:req, to_us(d), meta.conn.status})
  end

  def handle_event([:phoenix, :router_dispatch, :exception], %{duration: d}, meta, _) do
    if meta.route in @ingest_routes, do: insert({:req, to_us(d), 500})
  end

  def handle_event([:logflare, :repo, :query], measurements, meta, _) do
    if is_binary(meta.query) and String.starts_with?(meta.query, "SELECT") do
      insert({:q, origin(), meta.source || "?", to_us(Map.get(measurements, :total_time, 0))})
    end
  end

  defp init(config, parent) do
    :ets.new(@table, [:named_table, :public, :duplicate_bag, write_concurrency: true])
    Process.register(self(), @reporter)
    File.write!(config.csv, @csv_header)
    :ok = :telemetry.attach_many(@handler, @events, &__MODULE__.handle_event/4, nil)
    send(parent, {:ready, self()})
    loop(config)
  end

  defp loop(config) do
    receive do
      :stop -> report(config)
    after
      config.window_ms ->
        report(config)
        loop(config)
    end
  end

  defp report(config) do
    reqs = :ets.take(@table, :req)
    queries = :ets.take(@table, :q)

    durations = reqs |> Enum.map(fn {:req, us, _} -> us end) |> Enum.sort()
    errors = Enum.count(reqs, fn {:req, _, status} -> status >= 400 end)
    slow = Enum.count(durations, &(&1 >= config.slow_ms * 1_000))

    {background, request_path} =
      Enum.split_with(queries, fn {:q, origin, _, _} -> origin == :background end)

    background_ms = background |> Enum.map(fn {:q, _, _, us} -> us end) |> Enum.sum() |> div(1_000)

    tables =
      request_path
      |> Enum.frequencies_by(fn {:q, _, table, _} -> table end)
      |> Enum.sort_by(fn {_, count} -> count end, :desc)
      |> Enum.map_join(" ", fn {table, count} -> "#{table}=#{count}" end)

    row = [
      div(System.monotonic_time(:millisecond) - config.started_at, 1_000),
      length(durations),
      to_ms(percentile(durations, 0.5)),
      to_ms(percentile(durations, 0.99)),
      to_ms(List.last(durations) || 0),
      slow,
      errors,
      length(request_path),
      length(background),
      background_ms,
      tables
    ]

    File.write!(config.csv, Enum.join(row, ",") <> "\n", [:append])
    IO.puts(format_row(row))
  end

  defp insert(row) do
    if :ets.whereis(@table) != :undefined, do: :ets.insert(@table, row)
  end

  defp origin do
    case Process.get(:"$initial_call") do
      {module, _fun, _arity} when is_atom(module) ->
        if background_module?(module), do: :background, else: :other

      _ ->
        :other
    end
  end

  defp background_module?(Logflare.ContextCache.RefreshAhead), do: true
  defp background_module?(module), do: module |> Atom.to_string() |> String.ends_with?("Warmer")

  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    index = max(ceil(p * length(sorted)) - 1, 0)
    Enum.at(sorted, index)
  end

  defp to_us(native), do: System.convert_time_unit(native, :native, :microsecond)
  defp to_ms(us), do: Float.round(us / 1_000, 1)

  @columns ~w(t_s reqs p50_ms p99_ms max_ms slow errors rp_selects bg_selects bg_ms)
  defp format_header, do: format_row(@columns ++ ["tables"])

  defp format_row(row) do
    {numbers, [tables]} = Enum.split(row, length(@columns))
    Enum.map_join(numbers, "", &String.pad_leading(to_string(&1), 11)) <> "  " <> to_string(tables)
  end
end
