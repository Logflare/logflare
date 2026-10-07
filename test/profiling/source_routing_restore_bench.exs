defmodule RoutingRestoreBench do
  require Logger

  alias Logflare.ContextCache.Supervisor, as: CacheSupervisor
  alias Logflare.Rules.Cache
  alias Logflare.Rules.RoutingSnapshot, as: Snapshot
  alias Logflare.Rules.RoutingSnapshotStore, as: Store
  alias Logflare.Sources.SourceRouter.RulesTree

  def run do
    Code.ensure_loaded!(Snapshot)
    Code.ensure_loaded!(RulesTree)
    count = String.to_integer(System.get_env("ROUTING_RESTORE_RULES", "1000"))
    readers = String.to_integer(System.get_env("ROUTING_RESTORE_READERS", "32"))
    events = String.to_integer(System.get_env("ROUTING_RESTORE_EVENTS", "100"))
    repeats = String.to_integer(System.get_env("ROUTING_RESTORE_REPEATS", "5"))

    timings =
      for shape <- [:sparse, :dense], repeat <- 1..repeats do
        concurrent_misses(count, readers, events, shape) |> Map.put(:repeat, repeat)
      end

    footprints =
      for repeat <- 1..repeats do
        retained_headers(count, readers) |> Map.put(:repeat, repeat)
      end

    result = %{
      revision: System.get_env("ROUTING_RESTORE_REVISION"),
      system: %{
        otp: System.otp_release(),
        elixir: System.version(),
        schedulers: :erlang.system_info(:schedulers_online),
        architecture: to_string(:erlang.system_info(:system_architecture))
      },
      rules: count,
      readers: readers,
      events_per_reader: events,
      async_restore: function_exported?(Snapshot, :restore, 1),
      timings: timings,
      footprints: footprints
    }

    output = System.fetch_env!("ROUTING_RESTORE_OUTPUT")
    File.write!(output, Jason.encode!(result, pretty: true))
    Logger.info("Routing restore measurements saved to #{output}")
  end

  defp concurrent_misses(count, readers, events, shape) do
    reset_store(100_000)
    snapshots = for id <- 1..readers, do: publish(id, count)

    for snapshot <- snapshots, do: Store.delete(Store, snapshot.key)
    :sys.get_state(Store)
    parent = self()

    tasks =
      for snapshot <- snapshots do
        Task.async(fn -> resolve_reader(snapshot, count, events, shape, parent) end)
      end

    for _ <- tasks do
      receive do: ({:ready, _pid} -> :ok)
    end

    {reader_us, _} =
      :timer.tc(fn ->
        for task <- tasks, do: send(task.pid, :go)
        for task <- tasks, do: Task.await(task, :infinity)
      end)

    {drain_us, state} = :timer.tc(fn -> :sys.get_state(Store) end)

    %{
      shape: shape,
      reader_us: reader_us,
      drain_us: drain_us,
      resident_sources: :ets.info(state.sources, :size),
      pending_requests:
        if(Map.has_key?(state, :requests), do: :ets.info(state.requests, :size), else: 0)
    }
  end

  defp resolve_reader(snapshot, count, events, shape, parent) do
    ids = match_ids(snapshot, count, shape)
    expected = Enum.map(ids, &target_for(snapshot, &1))
    send(parent, {:ready, self()})
    receive do: (:go -> :ok)

    _local = Enum.reduce(1..events, snapshot, &resolve_event(&1, &2, ids, expected))
    :ok
  end

  defp resolve_event(_event, local, ids, expected) do
    {targets, local} = resolve(local, ids)
    if targets != expected, do: raise("mixed or missing routing targets")
    local
  end

  defp retained_headers(count, sources) do
    reset_store(8)
    empty = retained_bytes()
    snapshots = for id <- 1..sources, do: publish(id, count)
    state = :sys.get_state(Store)
    drain_retirement(snapshots)
    retained = retained_bytes() - empty
    header_count = Cachex.size!(Cache)

    for snapshot <- snapshots do
      ids = match_ids(snapshot, count, :sparse)
      {targets, _local} = resolve(snapshot, ids)

      if targets != Enum.map(ids, &target_for(snapshot, &1)),
        do: raise("eviction changed targets")
    end

    state_after = :sys.get_state(Store)
    drain_retirement(snapshots)

    %{
      before_routing_ets_bytes: retained,
      after_routing_ets_bytes: retained_bytes() - empty,
      headers_before_routing: header_count,
      headers_after_routing: Cachex.size!(Cache),
      store_sources_before: :ets.info(state.sources, :size),
      store_sources_after: :ets.info(state_after.sources, :size),
      store_estimated_bytes: state_after.estimated_bytes
    }
  end

  defp publish(id, count) do
    targets = for rule_id <- 1..count, do: {rule_id, id * 100_000 + rule_id, nil}
    entries = if positional?(), do: targets, else: Enum.map(targets, &{elem(&1, 0), &1})
    snapshot = Snapshot.new(id, entries)
    Cachex.put!(Cache, {:rules_tree_by_source_id, [id]}, {:cached, {[], snapshot}})
    snapshot
  end

  defp resolve(snapshot, ids) do
    case Snapshot.resolve_with_status(snapshot, ids) do
      {:ok, targets} ->
        {targets, snapshot}

      {:fallback, targets, decoded} ->
        {targets, restore(snapshot, decoded)}
    end
  end

  defp restore(snapshot, decoded) do
    if function_exported?(Snapshot, :restore, 1) do
      apply(Snapshot, :restore, [snapshot])
      Snapshot.with_decoded(snapshot, decoded)
    else
      repair(snapshot, decoded)
    end
  end

  defp repair(snapshot, decoded) do
    case apply(Cache, :repair_routing_snapshot, [elem(snapshot.key, 0), snapshot, decoded]) do
      {:repaired, repaired} -> repaired
      _ -> Snapshot.with_decoded(snapshot, decoded)
    end
  end

  defp match_ids(_snapshot, count, shape) do
    matched = if shape == :sparse, do: min(count, 8), else: count
    if positional?(), do: Enum.to_list(0..(matched - 1)), else: Enum.to_list(1..matched)
  end

  defp target_for(snapshot, id) do
    rule_id = if positional?(), do: id + 1, else: id
    {rule_id, elem(snapshot.key, 0) * 100_000 + rule_id, nil}
  end

  defp positional?, do: function_exported?(RulesTree, :build_routing, 1)

  defp reset_store(limit) do
    Cachex.clear!(Cache)
    Supervisor.terminate_child(CacheSupervisor, Store)

    if pid = Process.whereis(Store) do
      GenServer.stop(pid)
    end

    {:ok, _pid} = Store.start_link(name: Store, limit: limit)
  end

  defp drain_retirement(snapshots) do
    if function_exported?(Cache, :delete_routing_snapshot, 1) do
      Enum.each(snapshots, &retire_header/1)
    end

    :sys.get_state(Store)
  end

  defp retire_header(snapshot) do
    unless :ets.member(snapshot.table, snapshot.key) do
      apply(Cache, :delete_routing_snapshot, [snapshot.key])
    end
  end

  defp retained_bytes do
    store = Process.whereis(Store)

    :ets.all()
    |> Enum.filter(&(:ets.info(&1, :name) == Cache or :ets.info(&1, :owner) == store))
    |> Enum.map(&(:ets.info(&1, :memory) * :erlang.system_info(:wordsize)))
    |> Enum.sum()
  end
end

RoutingRestoreBench.run()
