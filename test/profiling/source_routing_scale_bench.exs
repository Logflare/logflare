defmodule RoutingScaleBench do
  alias Logflare.LogEvent
  alias Logflare.Lql.Parser
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.Sources.Source
  alias Logflare.Sources.SourceRouter.RulesTree

  @snapshot Logflare.Rules.RoutingSnapshot
  @store Logflare.Rules.RoutingSnapshotStore
  @target Logflare.Sources.SourceRouter.Target

  def run do
    Code.ensure_loaded!(RulesTree)
    sizes = integers("ROUTING_BENCH_RULES", "100,1000,10000")
    batches = integers("ROUTING_BENCH_BATCHES", "1,10,100")
    fallback? = System.get_env("ROUTING_BENCH_FALLBACK") == "1"
    compare? = System.get_env("ROUTING_BENCH_COMPARE_BATCH") == "1"

    inputs =
      for count <- sizes,
          shape <- [:zero, :one, :eight, :all],
          batch <- batches,
          not (shape == :all and count * batch > 100_000),
          not fallback? or shape in [:one, :eight],
          into: %{} do
        {"#{count} rules / #{shape} matches / #{batch} events", {count, shape, batch}}
      end

    scenarios = %{"production batch API" => &route/1}

    scenarios =
      if compare?, do: Map.put(scenarios, "fetch per event", &unprepared/1), else: scenarios

    suite =
      Benchee.run(scenarios,
        inputs: inputs,
        before_scenario: fn {count, shape, batch} ->
          fixture = fixture(count, shape)
          warm(fixture)
          expected = expected(fixture)

          ^expected =
            fixture |> Map.put(:events, [fixture.event]) |> route() |> hd() |> normalize()

          state = if fallback?, do: stale_state(fixture), else: nil
          Map.merge(fixture, %{events: List.duplicate(fixture.event, batch), state: state})
        end,
        after_scenario: fn fixture ->
          cleanup(fixture)
          fixture
        end,
        pre_check: :all_same,
        parallel: 1,
        warmup: number("ROUTING_BENCH_WARMUP", "1"),
        time: number("ROUTING_BENCH_TIME", "3"),
        memory_time: number("ROUTING_BENCH_MEMORY", "1"),
        print: [fast_warning: false]
      )

    output = System.get_env("ROUTING_BENCH_OUTPUT", "/tmp/source-routing-scale.json")

    rows =
      Enum.map(suite.scenarios, fn scenario ->
        %{
          input: scenario.input_name,
          job: scenario.name,
          mean_ns: scenario.run_time_data.statistics.average,
          median_ns: scenario.run_time_data.statistics.median,
          deviation: scenario.run_time_data.statistics.std_dev_ratio,
          memory_bytes: scenario.memory_usage_data.statistics.average
        }
      end)

    File.write!(
      output,
      Jason.encode!(%{system: Map.from_struct(suite.system), results: rows}, pretty: true)
    )

    IO.write(["ROUTING_RESULTS ", output, "\n"])

    unless fallback? do
      footprints =
        for count <- sizes, shape <- [:zero, :one, :eight, :all], do: footprint(count, shape)

      File.write!(output <> ".footprint.json", Jason.encode!(footprints, pretty: true))
    end
  end

  def fixture(count, shape) do
    source_id = 1_900_000_000 + count

    rules =
      for i <- 1..count do
        lql =
          case shape do
            :zero -> ~s(metadata.rule_id:"rule-#{i}" severity_number:>8)
            :one -> ~s(metadata.rule_id:"rule-#{i}" severity_number:>8)
            :eight -> "severity_number:>#{i}"
            :all -> "m.type:otel_log severity_number:>8"
          end

        {:ok, filters} = Parser.parse(lql)

        %Rule{
          id: source_id + i * 3,
          source_id: source_id,
          backend_id: source_id + i,
          lql_string: lql,
          lql_filters: filters,
          inserted_at: ~N[2026-10-05 00:00:00],
          updated_at: ~N[2026-10-05 00:00:00]
        }
      end

    event = %LogEvent{
      source_id: source_id,
      body: %{
        "metadata" => %{
          "type" => "otel_log",
          "rule_id" => if(shape == :zero, do: "absent", else: "rule-100")
        },
        "severity_number" => 9
      }
    }

    %{source: %Source{id: source_id}, rules: rules, event: event, shape: shape, count: count}
  end

  def warm(fixture, mode \\ :full) do
    cleanup(fixture)

    header =
      cond do
        function_exported?(RulesTree, :build_routing, 1) ->
          {tree, targets} = apply(RulesTree, :build_routing, [fixture.rules])

          {tree,
           apply(@snapshot, :new, [
             fixture.source.id,
             targets,
             [extra_estimated_bytes: :erlang.external_size(tree)]
           ])}

        Code.ensure_loaded?(@snapshot) ->
          tree = RulesTree.build(fixture.rules)
          targets = Enum.map(fixture.rules, &{&1.id, apply(@target, :from_rule, [&1])})

          {tree,
           apply(@snapshot, :new, [
             fixture.source.id,
             targets,
             [extra_estimated_bytes: :erlang.external_size(tree)]
           ])}

        true ->
          rules = if mode == :full, do: fixture.rules, else: matching(fixture)
          Cachex.put_many!(Rules.Cache, Enum.map(rules, &{{:get_rule, [&1.id]}, {:cached, &1}}))
          RulesTree.build(fixture.rules)
      end

    Cachex.put!(Rules.Cache, {:rules_tree_by_source_id, [fixture.source.id]}, {:cached, header})
    header
  end

  def route(fixture) do
    if function_exported?(RulesTree, :matching_rules_with_state, 3) do
      state = Map.get(fixture, :state) || apply(RulesTree, :prepare, [fixture.source])

      {targets, _state} =
        Enum.map_reduce(
          fixture.events,
          state,
          &apply(RulesTree, :matching_rules_with_state, [&1, fixture.source, &2])
        )

      targets
    else
      unprepared(fixture)
    end
  end

  def unprepared(fixture),
    do: Enum.map(fixture.events, &RulesTree.matching_rules(&1, fixture.source))

  def stale_state(fixture) do
    old = apply(RulesTree, :prepare, [fixture.source])
    warm(fixture)
    old
  end

  def cleanup(fixture) do
    Rules.Cache.bust_by(source_id: fixture.source.id)
    Cachex.clear!(Rules.Cache)
  end

  def footprint(count, shape) do
    fixture = fixture(count, shape)
    cleanup(fixture)
    empty = retained_bytes()
    warm(fixture, :representative)
    RulesTree.matching_rules(fixture.event, fixture.source)
    first = retained_bytes() - empty
    warm(fixture)
    full = retained_bytes() - empty
    cleanup(fixture)
    %{rules: count, shape: shape, representative_ets_bytes: first, fully_warmed_ets_bytes: full}
  end

  def retained_bytes do
    store_pid = Process.whereis(@store)
    word_size = :erlang.system_info(:wordsize)

    :ets.all()
    |> Enum.filter(fn table ->
      :ets.info(table, :name) == Rules.Cache or
        (store_pid != nil and :ets.info(table, :owner) == store_pid)
    end)
    |> Enum.map(&(:ets.info(&1, :memory) * word_size))
    |> Enum.sum()
  end

  def matching(fixture) do
    count =
      case fixture.shape do
        :zero -> 0
        :one -> 1
        :eight -> 8
        :all -> fixture.count
      end

    if fixture.shape == :one,
      do: [Enum.at(fixture.rules, 99)],
      else: Enum.take(fixture.rules, count)
  end

  def expected(fixture),
    do: matching(fixture) |> Enum.map(&{&1.id, &1.backend_id, &1.sink}) |> Enum.sort()

  def normalize(targets),
    do:
      Enum.map(targets, fn
        %Rule{} = rule -> {rule.id, rule.backend_id, rule.sink}
        target -> target
      end)
      |> Enum.sort()

  def integers(key, default),
    do: System.get_env(key, default) |> String.split(",") |> Enum.map(&String.to_integer/1)

  def number(key, default), do: System.get_env(key, default) |> Float.parse() |> elem(0)
end

RoutingScaleBench.run()
