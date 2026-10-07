System.put_env("ROUTING_BENCH_LIBRARY", "1")

Code.require_file(
  System.get_env(
    "ROUTING_BENCH_LIBRARY_PATH",
    Path.join(__DIR__, "source_routing_scale_bench.exs")
  )
)

defmodule RoutingMainBench do
  alias Logflare.ContextCache.Supervisor, as: CacheSupervisor
  alias Logflare.Rules.Cache
  alias Logflare.Sources.SourceRouter.RulesTree

  @store Logflare.Rules.RoutingSnapshotStore

  @spec run() :: :ok
  def run do
    Code.ensure_loaded!(RulesTree)

    result =
      case System.get_env("ROUTING_MAIN_MODE", "concurrent") do
        "concurrent" -> concurrent()
        "retained" -> retained()
      end

    result = Map.put(result, :revision, System.fetch_env!("ROUTING_MAIN_REVISION"))
    File.write!(System.fetch_env!("ROUTING_BENCH_OUTPUT"), Jason.encode!(result, pretty: true))
  end

  @spec concurrent() :: map()
  defp concurrent do
    inputs =
      for readers <- RoutingScaleBench.integers("ROUTING_MAIN_READERS", "1,8,32"),
          sources <- RoutingScaleBench.integers("ROUTING_MAIN_SOURCES", "1,8"),
          batch <- RoutingScaleBench.integers("ROUTING_BENCH_BATCHES", "1,10"),
          into: %{} do
        {"#{readers} readers / #{sources} sources / #{batch} events", {readers, sources, batch}}
      end

    suite =
      Benchee.run(%{"concurrent hot routing wave" => &wave/1},
        inputs: RoutingScaleBench.filter_inputs(inputs),
        before_scenario: &prepare_wave/1,
        after_scenario: fn data ->
          RoutingScaleBench.cleanup(hd(data.fixtures))
          data
        end,
        warmup: RoutingScaleBench.number("ROUTING_BENCH_WARMUP", "1"),
        time: RoutingScaleBench.number("ROUTING_BENCH_TIME", "3"),
        memory_time: 0,
        parallel: 1,
        print: [fast_warning: false]
      )

    rows =
      Enum.map(suite.scenarios, fn scenario ->
        stats = scenario.run_time_data.statistics

        %{
          input: scenario.input_name,
          mean_ns: stats.average,
          median_ns: stats.median,
          p99_ns: Map.get(stats.percentiles, 99),
          deviation: stats.std_dev_ratio,
          samples: stats.sample_size
        }
      end)

    %{system: Map.from_struct(suite.system), rules: 10_000, results: rows}
  end

  @spec prepare_wave({pos_integer(), pos_integer(), pos_integer()}) :: map()
  defp prepare_wave({readers, sources, batch}) do
    fixtures =
      for index <- 1..sources do
        RoutingScaleBench.fixture(10_000, :one)
        |> clone(index)
        |> then(&Map.put(&1, :events, List.duplicate(&1.event, batch)))
      end

    RoutingScaleBench.cleanup(hd(fixtures))

    for fixture <- fixtures do
      RoutingScaleBench.publish_header(fixture)
      expected = RoutingScaleBench.expected(fixture)
      results = RoutingScaleBench.route(fixture)
      true = Enum.all?(results, &(RoutingScaleBench.normalize(&1) == expected))
    end

    %{fixtures: fixtures, readers: readers, batch: batch}
  end

  @spec wave(map()) :: :ok
  defp wave(data) do
    parent = self()
    batch = data.batch

    tasks =
      for index <- 0..(data.readers - 1) do
        fixture = Enum.at(data.fixtures, rem(index, length(data.fixtures)))

        Task.async(fn ->
          send(parent, {:routing_ready, self()})
          receive do: (:routing_go -> :ok)
          ^batch = length(RoutingScaleBench.route(fixture))
          :ok
        end)
      end

    for _ <- tasks do
      receive do: ({:routing_ready, _pid} -> :ok)
    end

    for task <- tasks, do: send(task.pid, :routing_go)
    for task <- tasks, do: Task.await(task, 120_000)
    :ok
  end

  @spec retained() :: map()
  defp retained do
    rows =
      Enum.map(
        RoutingScaleBench.integers("ROUTING_BENCH_RULES", "100,1000,10000"),
        &retained_count/1
      )

    %{
      system: %{
        elixir: System.version(),
        otp: System.otp_release(),
        schedulers: :erlang.system_info(:schedulers_online)
      },
      results: rows
    }
  end

  @spec retained_count(pos_integer()) :: map()
  defp retained_count(count) do
    reset_store()

    fixtures =
      for index <- 1..32, do: count |> RoutingScaleBench.fixture(:one) |> clone(index)

    RoutingScaleBench.cleanup(hd(fixtures))
    empty = memory()

    rounds =
      for round <- 1..3 do
        Enum.each(fixtures, &RoutingScaleBench.publish_header/1)
        drain_store()
        after_publication = memory()

        for fixture <- fixtures do
          result = RoutingScaleBench.route(Map.put(fixture, :events, [fixture.event]))

          true =
            RoutingScaleBench.normalize(hd(result)) == RoutingScaleBench.expected(fixture)
        end

        drain_store()
        %{round: round, after_publication: after_publication, after_read: memory()}
      end

    Enum.each(fixtures, &Cache.bust_by(source_id: &1.source.id))
    drain_store()
    invalidated = memory()
    RoutingScaleBench.cleanup(hd(fixtures))

    %{
      rules: count,
      sources: 32,
      store_limit: 8,
      empty: empty,
      rounds: rounds,
      after_invalidation: invalidated,
      after_clear: memory()
    }
  end

  @spec clone(map(), pos_integer()) :: map()
  defp clone(fixture, index) do
    offset = index * 1_000_000
    source = %{fixture.source | id: fixture.source.id + offset}

    rules =
      Enum.map(
        fixture.rules,
        &%{&1 | source_id: source.id, id: &1.id + offset, backend_id: &1.backend_id + offset}
      )

    %{fixture | source: source, rules: rules, event: %{fixture.event | source_id: source.id}}
  end

  @spec reset_store() :: :ok
  defp reset_store do
    Cachex.clear!(Cache)

    if Process.whereis(@store) do
      :ok = Supervisor.terminate_child(CacheSupervisor, @store)
      if pid = Process.whereis(@store), do: GenServer.stop(pid)
      {:ok, _pid} = apply(@store, :start_link, [[name: @store, limit: 8, max_bytes: :infinity]])
    end

    :ok
  end

  @spec drain_store() :: term()
  defp drain_store do
    if pid = Process.whereis(@store), do: :sys.get_state(pid)
  end

  @spec memory() :: map()
  defp memory do
    state = drain_store()
    :erlang.garbage_collect(self())
    if pid = Process.whereis(@store), do: :erlang.garbage_collect(pid)

    headers =
      for {:rules_tree_by_source_id, [_]} = key <- Cachex.keys!(Cache),
          do: Cachex.get!(Cache, key)

    header_binary_bytes =
      Enum.reduce(headers, 0, fn
        {:cached, {_tree, %{encoded: encoded} = snapshot}}, bytes ->
          bytes + byte_size(encoded) + byte_size(Map.get(snapshot, :index, <<>>))

        _, bytes ->
          bytes
      end)

    %{
      ets_bytes: RoutingScaleBench.retained_bytes(),
      cache_entries: Cachex.size!(Cache),
      headers: length(headers),
      header_binary_bytes: header_binary_bytes,
      resident_sources: if(state, do: :ets.info(state.sources, :size)),
      store_estimated_bytes: if(state, do: state.estimated_bytes),
      vm_binary_bytes: :erlang.memory(:binary),
      vm_total_bytes: :erlang.memory(:total),
      store_process_bytes:
        if(Process.whereis(@store), do: elem(Process.info(Process.whereis(@store), :memory), 1))
    }
  end
end

RoutingMainBench.run()
