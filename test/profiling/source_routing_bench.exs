alias Logflare.Sources.SourceRouter
alias Logflare.Backends
alias Logflare.Sources
alias Logflare.Rules
alias Logflare.Rules.Rule
alias Logflare.Users
alias Logflare.Lql.Rules.FilterRule
alias Logflare.Lql.Parser

import Logflare.Factory

pid =
  Ecto.Adapters.SQL.Sandbox.start_owner!(Logflare.Repo,
    shared: true,
    ownership_timeout: 1_200_000
  )

insert(:plan)
user = insert(:user)

log_event = fn source_id ->
  build(:log_event,
    attributes: %{
      "_http_method" => "GET",
      "_http_status_code" => 201,
      "_http_target" => "/api/v1/resource/0",
      "_k8s_container_name" => "otelgen",
      "_k8s_namespace_name" => "default",
      "_k8s_pod_name" => "otelgen-pod-e5bd0f2d",
      "_service_name" => "otelgen",
      "phase" => "finish",
      "span_id" => "eb904945727e4667",
      "trace_flags" => "01",
      "trace_id" => "2ba9cd502612a2ac01b0d4b50cb8d1f5",
      "worker_id" => "0"
    },
    body: "Log 0: Info phase: finish",
    event_message: "Log 0: Info phase: finish",
    metadata: %{"type" => "otel_log", "rule_id" => "rule-100"},
    resource: %{
      "host" => %{"name" => "node-1"},
      "k8s" => %{
        "container" => %{"name" => "otelgen"},
        "namespace" => %{"name" => "default"},
        "pod" => %{"name" => "otelgen-pod-c1e9717d"}
      },
      "service" => %{"name" => "otelgen"}
    },
    scope: %{"name" => "otelgen"},
    severity_number: 9,
    severity_text: "Info",
    timestamp: 1_765_540_359_181_556,
    source_id: source_id
  )
end

one_matching = fn rules_num ->
  rules =
    for i <- 1..rules_num do
      backend = insert(:backend, user: user)

      lql_string = "metadata.rule_id:rule-#{i} severity_number:>8"
      {:ok, filters} = Parser.parse(lql_string)

      %Rule{
        lql_string: lql_string,
        lql_filters: filters,
        backend_id: backend.id
      }
    end

  source = insert(:source, user: user, rules: rules)

  {log_event.(source.id), source}
end

few_matching = fn rules_num ->
  rules =
    for i <- 1..rules_num do
      backend = insert(:backend, user: user)

      lql_string = "severity_number:>#{i}"
      {:ok, filters} = Parser.parse(lql_string)

      %Rule{
        lql_string: lql_string,
        lql_filters: filters,
        backend_id: backend.id
      }
    end

  source = insert(:source, user: user, rules: rules)

  {log_event.(source.id), source}
end

all_matching = fn rules_num ->
  rules =
    for i <- 1..rules_num do
      backend = insert(:backend, user: user)

      lql_string = "m.type:otel_log severity_number:>8"
      {:ok, filters} = Parser.parse(lql_string)

      %Rule{
        lql_string: lql_string,
        lql_filters: filters,
        backend_id: backend.id
      }
    end

  source = insert(:source, user: user, rules: rules)

  {log_event.(source.id), source}
end

warmup = fn source ->
  rules = Rules.Cache.list_rules(source)
  for rule <- rules, do: Rules.Cache.get_rule(rule.id)
  _rule_set = Rules.Cache.rules_tree_by_source_id(source.id)
end

Benchee.run(
  %{
    "Reference" => fn {event, source} ->
      SourceRouter.Sequential.matching_rules(event, source) |> MapSet.new()
    end,
    "RulesTree" => fn {event, source} ->
      SourceRouter.RulesTree.matching_rules(event, source) |> MapSet.new()
    end
  },
  before_scenario: fn {event, source} ->
    Cachex.clear!(Rules.Cache)
    warmup.(source)
    {event, source}
  end,
  inputs: %{
    "source with 100 rules and few matching" => few_matching.(100),
    "source with 100 rules and one matching" => one_matching.(100),
    "source with 100 rules and all matching" => all_matching.(100),
    "source with 1000 rules and few matching" => few_matching.(1000),
    "source with 1000 rules and one matching" => one_matching.(1000),
    "source with 1000 rules and all matching" => all_matching.(1000)
  },
  # save: [path: Path.join(__DIR__, "source_routing.benchee")],
  pre_check: :all_same,
  # profile_after: :tprof,
  time: 5,
  memory_time: 2
)

# Exclude warmup when profiling
# input_100 = {_, source} = few_matching.(100)
# warmup.(source)
# input_1000 = {_, source} = few_matching.(1000)
# warmup.(source)
#
# Benchee.run(
#   %{
#     "Reference" => fn {event, source} ->
#       SourceRouter.Sequential.matching_rules(event, source) |> MapSet.new()
#     end,
#     "GroupedRuleset" => fn {event, source} ->
#       SourceRouter.GroupedRuleset.matching_rules(event, source) |> MapSet.new()
#     end
#   },
#   inputs: %{
#     "source with 100 rules and few matching" => input_100,
#     "source with 1000 rules and few matching" => input_1000
#   },
#   profile_after: :tprof,
#   time: 1,
#   memory_time: 0
# )

Ecto.Adapters.SQL.Sandbox.stop_owner(pid)

# ##### With input source with 100 rules and all matching #####
# Name                ips        average  deviation         median         99th %
# Reference        7.61 K      131.49 μs    ±16.65%      121.92 μs      186.29 μs
# RulesTree        6.89 K      145.15 μs     ±7.63%      144.79 μs      173.54 μs

# Comparison:
# Reference        7.61 K
# RulesTree        6.89 K - 1.10x slower +13.66 μs

# Memory usage statistics:

# Name         Memory usage
# Reference        31.41 KB
# RulesTree       257.98 KB - 8.21x memory usage +226.56 KB

# **All measurements for memory usage were the same**

# ##### With input source with 100 rules and few matching #####
# Name                ips        average  deviation         median         99th %
# RulesTree       78.11 K       12.80 μs    ±20.06%       12.04 μs       19.96 μs
# Reference       15.25 K       65.55 μs    ±16.22%       61.75 μs       90.63 μs

# Comparison:
# RulesTree       78.11 K
# Reference       15.25 K - 5.12x slower +52.75 μs

# Memory usage statistics:

# Name         Memory usage
# RulesTree        27.97 KB
# Reference        13.37 KB - 0.48x memory usage -14.60156 KB

# **All measurements for memory usage were the same**

# ##### With input source with 100 rules and one matching #####
# Name                ips        average  deviation         median         99th %
# RulesTree       70.92 K       14.10 μs    ±25.44%       13.13 μs       25.71 μs
# Reference       12.73 K       78.56 μs    ±16.59%       72.50 μs      105.88 μs

# Comparison:
# RulesTree       70.92 K
# Reference       12.73 K - 5.57x slower +64.46 μs

# Memory usage statistics:

# Name         Memory usage
# RulesTree        31.34 KB
# Reference        17.02 KB - 0.54x memory usage -14.32813 KB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and all matching #####
# Name                ips        average  deviation         median         99th %
# Reference        834.73        1.20 ms    ±10.72%        1.12 ms        1.42 ms
# RulesTree        676.95        1.48 ms     ±2.70%        1.47 ms        1.58 ms

# Comparison:
# Reference        834.73
# RulesTree        676.95 - 1.23x slower +0.28 ms

# Memory usage statistics:

# Name         Memory usage
# Reference         0.30 MB
# RulesTree         2.54 MB - 8.56x memory usage +2.24 MB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and few matching #####
# Name                ips        average  deviation         median         99th %
# RulesTree       17.30 K       57.81 μs    ±19.81%       52.79 μs       87.67 μs
# Reference        1.60 K      623.80 μs    ±14.47%      678.58 μs      787.15 μs

# Comparison:
# RulesTree       17.30 K
# Reference        1.60 K - 10.79x slower +565.98 μs

# Memory usage statistics:

# Name         Memory usage
# RulesTree        18.20 KB
# Reference       118.84 KB - 6.53x memory usage +100.63 KB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and one matching #####
# Name                ips        average  deviation         median         99th %
# RulesTree        6.57 K      152.10 μs     ±5.86%      151.71 μs      175.08 μs
# Reference        1.36 K      733.64 μs    ±15.34%      660.02 μs      944.25 μs

# Comparison:
# RulesTree        6.57 K
# Reference        1.36 K - 4.82x slower +581.54 μs

# Memory usage statistics:

# Name         Memory usage
# RulesTree       351.91 KB
# Reference       157.64 KB - 0.45x memory usage -194.26563 KB
