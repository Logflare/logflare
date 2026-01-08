alias Logflare.Sources.SourceRouter.GroupedRuleset
alias Logflare.Sources.SourceRouter
alias Logflare.Backends
alias Logflare.Sources
alias Logflare.Rules
alias Logflare.Rules.Rule
alias Logflare.Users
alias Logflare.Lql.Rules.FilterRule

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

      %Rule{
        lql_filters: [
          %FilterRule{
            path: "metadata.rule_id",
            operator: :=,
            value: "rule-#{i}",
            modifiers: %{}
          },
          %FilterRule{
            path: "severity_number",
            operator: :>,
            value: 8,
            modifiers: %{}
          }
        ],
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

      %Rule{
        lql_filters: [
          %FilterRule{
            path: "severity_number",
            operator: :>,
            value: i,
            modifiers: %{}
          }
        ],
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

      %Rule{
        lql_filters: [
          %FilterRule{
            path: "metadata.type",
            operator: :=,
            value: "otel_log",
            modifiers: %{}
          },
          %FilterRule{
            path: "severity_number",
            operator: :>,
            value: 8,
            modifiers: %{}
          }
        ],
        backend_id: backend.id
      }
    end

  source = insert(:source, user: user, rules: rules)

  {log_event.(source.id), source}
end

warmup = fn source ->
  rules = Rules.Cache.list_rules(source)
  for rule <- rules, do: Rules.Cache.get_rule(rule.id)
  _rule_set = Rules.Cache.ruleset_by_source_id(source.id)
end

Benchee.run(
  %{
    "Reference" => fn {event, source} ->
      SourceRouter.Sequential.matching_rules(event, source) |> MapSet.new()
    end,
    "GroupedRuleset" => fn {event, source} ->
      SourceRouter.GroupedRuleset.matching_rules(event, source) |> MapSet.new()
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
# Name                     ips        average  deviation         median         99th %
# Reference             7.61 K      131.49 μs    ±12.09%      122.42 μs      171.58 μs
# GroupedRuleset        6.79 K      147.18 μs     ±7.49%      146.58 μs      179.88 μs

# Comparison:
# Reference             7.61 K
# GroupedRuleset        6.79 K - 1.12x slower +15.69 μs

# Memory usage statistics:

# Name              Memory usage
# Reference             31.48 KB
# GroupedRuleset       257.66 KB - 8.19x memory usage +226.18 KB

# **All measurements for memory usage were the same**

# ##### With input source with 100 rules and few matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset       66.82 K       14.97 μs    ±30.00%       13.79 μs       30.25 μs
# Reference            15.27 K       65.48 μs    ±16.44%       60.08 μs       89.83 μs

# Comparison:
# GroupedRuleset       66.82 K
# Reference            15.27 K - 4.38x slower +50.52 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset        36.18 KB
# Reference             13.37 KB - 0.37x memory usage -22.81250 KB

# **All measurements for memory usage were the same**

# ##### With input source with 100 rules and one matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset       62.28 K       16.06 μs    ±27.06%       14.96 μs       29.96 μs
# Reference            12.32 K       81.14 μs    ±15.98%       72.96 μs      104.47 μs

# Comparison:
# GroupedRuleset       62.28 K
# Reference            12.32 K - 5.05x slower +65.08 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset           53 KB
# Reference             17.02 KB - 0.32x memory usage -35.98438 KB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and all matching #####
# Name                     ips        average  deviation         median         99th %
# Reference             786.30        1.27 ms    ±10.42%        1.19 ms        1.49 ms
# GroupedRuleset        575.46        1.74 ms    ±15.84%        1.71 ms        2.15 ms

# Comparison:
# Reference             786.30
# GroupedRuleset        575.46 - 1.37x slower +0.47 ms

# Memory usage statistics:

# Name              Memory usage
# Reference              0.30 MB
# GroupedRuleset         2.48 MB - 8.36x memory usage +2.18 MB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and few matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset       10.10 K       99.00 μs     ±8.58%       98.17 μs      120.50 μs
# Reference             1.52 K      658.55 μs    ±18.78%      678.81 μs      855.72 μs

# Comparison:
# GroupedRuleset       10.10 K
# Reference             1.52 K - 6.65x slower +559.55 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset        96.41 KB
# Reference            118.84 KB - 1.23x memory usage +22.43 KB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and one matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset        6.08 K      164.55 μs    ±14.23%      151.71 μs      239.29 μs
# Reference             1.32 K      755.20 μs    ±15.50%      673.29 μs      967.99 μs

# Comparison:
# GroupedRuleset        6.08 K
# Reference             1.32 K - 4.59x slower +590.65 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset       420.54 KB
# Reference            157.64 KB - 0.37x memory usage -262.89844 KB
