alias Logflare.LogEvent
alias Logflare.Lql.Parser
alias Logflare.Rules
alias Logflare.Rules.Rule
alias Logflare.Rules.RoutingSnapshot
alias Logflare.Sources.Source
alias Logflare.Sources.SourceRouter.RulesTree
alias Logflare.Sources.SourceRouter.Target

source_id = System.unique_integer([:positive])
source = %Source{id: source_id}

rules =
  for i <- 1..1_000 do
    {:ok, filters} = Parser.parse("severity_number:>#{i}")

    %Rule{
      id: i,
      source_id: source_id,
      backend_id: i,
      lql_string: "severity_number:>#{i}",
      lql_filters: filters
    }
  end

tree = RulesTree.build(rules)
targets = rules |> Enum.sort_by(& &1.id) |> Enum.map(&{&1.id, Target.from_rule(&1)})

snapshot =
  RoutingSnapshot.new(source_id, targets, extra_estimated_bytes: :erlang.external_size(tree))

Cachex.put!(
  Rules.Cache,
  {:rules_tree_by_source_id, [source_id]},
  {:cached, {tree, snapshot}}
)

event = %LogEvent{body: %{"severity_number" => 9}, source_id: source_id}
8 = event |> RulesTree.matching_rule_ids(tree) |> length()

Benchee.run(
  %{
    "fetch snapshot per event" => fn events ->
      Enum.map(events, &RulesTree.matching_rules(&1, source))
    end,
    "fetch snapshot once per batch" => fn events ->
      prepared = RulesTree.prepare(source)
      Enum.map(events, &RulesTree.matching_rules(&1, source, prepared))
    end
  },
  inputs: %{
    "10 events, 1000 rules, 8 matches" => List.duplicate(event, 10),
    "100 events, 1000 rules, 8 matches" => List.duplicate(event, 100)
  },
  pre_check: :all_same,
  warmup: 1,
  time: 3,
  memory_time: 1,
  print: [fast_warning: false]
)
