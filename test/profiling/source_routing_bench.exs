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
# Name                     ips        average  deviation         median         99th %
# Reference             7.83 K      127.73 μs    ±11.60%      121.17 μs      157.75 μs
# GroupedRuleset        6.93 K      144.21 μs     ±6.58%      144.42 μs      169.96 μs

# Comparison:
# Reference             7.83 K
# GroupedRuleset        6.93 K - 1.13x slower +16.48 μs

# Memory usage statistics:

# Name              Memory usage
# Reference             31.45 KB
# GroupedRuleset       256.98 KB - 8.17x memory usage +225.53 KB

# **All measurements for memory usage were the same**

# ##### With input source with 100 rules and few matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset       69.43 K       14.40 μs    ±17.41%       14.25 μs       22.71 μs
# Reference            15.45 K       64.74 μs    ±15.98%       60.21 μs       88.83 μs

# Comparison:
# GroupedRuleset       69.43 K
# Reference            15.45 K - 4.50x slower +50.34 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset        32.47 KB
# Reference             13.37 KB - 0.41x memory usage -19.10156 KB

# **All measurements for memory usage were the same**

# ##### With input source with 100 rules and one matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset       64.06 K       15.61 μs    ±16.13%       14.92 μs       21.96 μs
# Reference            12.21 K       81.88 μs    ±17.30%       74.50 μs      106.67 μs

# Comparison:
# GroupedRuleset       64.06 K
# Reference            12.21 K - 5.25x slower +66.27 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset        51.73 KB
# Reference             17.02 KB - 0.33x memory usage -34.71094 KB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and all matching #####
# Name                     ips        average  deviation         median         99th %
# Reference             804.78        1.24 ms    ±11.21%        1.16 ms        1.50 ms
# GroupedRuleset        619.46        1.61 ms    ±11.18%        1.53 ms        2.06 ms

# Comparison:
# Reference             804.78
# GroupedRuleset        619.46 - 1.30x slower +0.37 ms

# Memory usage statistics:

# Name              Memory usage
# Reference              0.30 MB
# GroupedRuleset         2.48 MB - 8.35x memory usage +2.18 MB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and few matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset       13.05 K       76.66 μs    ±29.92%       62.42 μs      114.80 μs
# Reference             1.56 K      639.28 μs    ±14.34%      685.17 μs      839.95 μs

# Comparison:
# GroupedRuleset       13.05 K
# Reference             1.56 K - 8.34x slower +562.63 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset        72.97 KB
# Reference            118.84 KB - 1.63x memory usage +45.87 KB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and one matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset        6.67 K      149.84 μs    ±13.81%      143.96 μs      227.54 μs
# Reference             1.33 K      753.69 μs    ±15.77%      673.42 μs     1054.53 μs

# Comparison:
# GroupedRuleset        6.67 K
# Reference             1.33 K - 5.03x slower +603.85 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset       405.34 KB
# Reference            157.64 KB - 0.39x memory usage -247.70313 KB
