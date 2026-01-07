alias Logflare.Sources.SourceRouter.GroupedRuleset
alias Logflare.Sources.SourceRouter
alias Logflare.Backends
alias Logflare.Sources
alias Logflare.Rules
alias Logflare.Rules.Rule
alias Logflare.Users
alias Logflare.Lql.Rules.FilterRule

import Logflare.Factory

pid = Ecto.Adapters.SQL.Sandbox.start_owner!(Logflare.Repo, shared: true)

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
    # warmup cache
    rules = Rules.Cache.list_rules(source)
    for rule <- rules, do: Rules.Cache.get_rule(rule.id)
    _rule_set = Rules.Cache.ruleset_by_source_id(source.id)
    {event, source}
  end,
  inputs: %{
    "source with 1000 rules and one matching" => one_matching.(1000),
    "source with 1000 rules and all matching" => all_matching.(1000)
  },
  pre_check: :all_same,
  profile_after: :tprof,
  time: 5,
  memory_time: 2
)

Ecto.Adapters.SQL.Sandbox.stop_owner(pid)

# ##### With input source with 1000 rules and all matching #####
# Name                     ips        average  deviation         median         99th %
# Reference             840.41        1.19 ms    ±10.85%        1.11 ms        1.42 ms
# GroupedRuleset        427.54        2.34 ms    ±22.63%        2.24 ms        5.11 ms

# Comparison:
# Reference             840.41
# GroupedRuleset        427.54 - 1.97x slower +1.15 ms

# Memory usage statistics:

# Name              Memory usage
# Reference              0.30 MB
# GroupedRuleset         3.22 MB - 10.83x memory usage +2.92 MB

# **All measurements for memory usage were the same**

# ##### With input source with 1000 rules and one matching #####
# Name                     ips        average  deviation         median         99th %
# GroupedRuleset        5.60 K      178.43 μs    ±10.19%      174.33 μs      217.29 μs
# Reference             1.37 K      730.99 μs    ±15.28%      657.46 μs      939.00 μs

# Comparison:
# GroupedRuleset        5.60 K
# Reference             1.37 K - 4.10x slower +552.55 μs

# Memory usage statistics:

# Name              Memory usage
# GroupedRuleset       420.77 KB
# Reference            157.64 KB - 0.37x memory usage -263.13281 KB

# **All measurements for memory usage were the same**

# Profiling Reference with tprof...

# Profile results of #PID<0.16312.0>
# #                                                                                  CALLS      % TIME µS/CALL
# Total                                                                              53169 100.00 3622    0.07
# List.last/1                                                                            1   0.00    0    0.00
# List.last/2                                                                            1   0.00    0    0.00
# Kernel.struct!/2                                                                       1   0.00    0    0.00
# Kernel.validate_struct!/3                                                              1   0.00    0    0.00
# :erts_internal.map_next/3                                                              1   0.00    0    0.00
# :erlang.atom_to_binary/1                                                               2   0.00    0    0.00
# :erlang.atom_to_binary/2                                                               2   0.00    0    0.00
# :erlang.whereis/1                                                                      1   0.00    0    0.00
# MapSet.new/1                                                                           1   0.00    0    0.00
# Module.concat/2                                                                        1   0.00    0    0.00
# :maps.from_list/1                                                                      1   0.00    0    0.00
# :maps.fold/3                                                                           1   0.00    0    0.00
# :maps.iterator/1                                                                       1   0.00    0    0.00
# :maps.iterator/2                                                                       1   0.00    0    0.00
# :maps.next/1                                                                           1   0.00    0    0.00
# :maps.try_next/2                                                                      13   0.00    0    0.00
# Enum.each/2                                                                            1   0.00    0    0.00
# Enum.filter/2                                                                          2   0.00    0    0.00
# Enum.filter_list/2                                                                     3   0.00    0    0.00
# Enum.reduce/3                                                                          2   0.00    0    0.00
# Enum.to_list/1                                                                         1   0.00    0    0.00
# anonymous fn/4 in Enum.reduce/3                                                       13   0.00    0    0.00
# Enum."-each/2-lists^foreach/1-0-"/2                                                    2   0.00    0    0.00
# Map.take/2                                                                             1   0.00    0    0.00
# :sets.from_list/2                                                                      1   0.00    0    0.00
# Cachex.Services.Informant.broadcast/2                                                  1   0.00    0    0.00
# Cachex.Services.Informant.broadcast/3                                                  1   0.00    0    0.00
# Cachex.Services.Informant.broadcast_action/3                                           2   0.00    0    0.00
# Cachex.Services.Informant.notify/3                                                     2   0.00    0    0.00
# anonymous fn/2 in Cachex.Services.Informant.notify/3                                   1   0.00    0    0.00
# anonymous fn/2 in Cachex.Services.Informant.broadcast_action/3                         1   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# Cachex.Actions.Fetch.execute/4                                                         1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_each/3                                               1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_scenario/2                                           1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_each/2                                              1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_function/2                                          4   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_scenario/2                                          1   0.00    0    0.00
# Logflare.Sources.SourceRouter.Sequential.matching_rules/2                              1   0.00    0    0.00
# Cachex.Services.Janitor.expired?/1                                                     1   0.00    0    0.00
# Cachex.Services.Janitor.expired?/2                                                     1   0.00    0    0.00
# Benchee.Benchmark.Runner.collect_return_value/2                                        1   0.00    0    0.00
# Benchee.Benchmark.Runner.main_function/2                                               1   0.00    0    0.00
# Benchee.Benchmark.Runner.run_once/2                                                    1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.Runner.main_function/2                             1   0.00    0    0.00
# :elixir_aliases.concat/1                                                               1   0.00    0    0.00
# :elixir_aliases.do_concat/1                                                            2   0.00    0    0.00
# :elixir_aliases.to_partial/1                                                           1   0.00    0    0.00
# Cachex.Router.route/3                                                                  1   0.00    0    0.00
# Cachex.Router.route_local/3                                                            1   0.00    0    0.00
# :os.system_time/1                                                                      1   0.00    0    0.00
# Cachex.Actions.Get.execute/3                                                           1   0.00    0    0.00
# Cachex.Services.Overseer.get/1                                                         1   0.00    0    0.00
# Cachex.Services.Overseer.retrieve/1                                                    1   0.00    0    0.00
# Cachex.Services.Overseer.with/2                                                        1   0.00    0    0.00
# Cachex.Stats.actions/0                                                                 2   0.00    0    0.00
# Cachex.Stats.async?/0                                                                  1   0.00    0    0.00
# Cachex.Stats.type/0                                                                    1   0.00    0    0.00
# Cachex.fetch/3                                                                         1   0.00    0    0.00
# Cachex.fetch/4                                                                         1   0.00    0    0.00
# anonymous fn/4 in Cachex.fetch/4                                                       1   0.00    0    0.00
# :lists.reverse/1                                                                       1   0.00    0    0.00
# Logflare.ContextCache.apply_fun/3                                                      2   0.00    0    0.00
# Logflare.ContextCache.cache_name/1                                                     1   0.00    0    0.00
# Logflare.Rules.Cache.apply_repo_fun/2                                                  1   0.00    0    0.00
# Logflare.Rules.Cache.list_by_source_id/1                                               1   0.00    0    0.00
# Logflare.Rules.Cache.list_rules/1                                                      1   0.00    0    0.00
# Benchee.Benchmark.BenchmarkConfig.__struct__/1                                         1   0.00    0    0.00
# Benchee.Benchmark.BenchmarkConfig.from/1                                               1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.BenchmarkConfig.__struct__/1                      13   0.00    0    0.00
# Process.get/2                                                                          1   0.00    0    0.00
# :maps.fold_1/4                                                                        14   0.03    1    0.07
# anonymous fn/2 in Benchee.Profile.run/4                                                1   0.03    1    1.00
# Map.take/3                                                                            14   0.03    1    0.07
# :elixir_aliases.do_concat/2                                                            2   0.03    1    0.50
# :erlang.binary_to_atom/2                                                               1   0.08    3    3.00
# :lists.reverse/2                                                                       1   0.11    4    4.00
# Kernel.>/2                                                                          1000   0.69   25    0.03
# Enum.all?/2                                                                         1000   0.72   26    0.03
# Logflare.Sources.SourceRouter.Sequential.route_with_lql_rules?/2                    1000   0.72   26    0.03
# :erlang.integer_to_binary/1                                                         1000   1.33   48    0.05
# Access.get/3                                                                        2001   1.38   50    0.02
# Enum.any?/2                                                                         2000   1.41   51    0.03
# Enum."-reduce/3-lists^foldl/2-0-"/3                                                 1001   1.41   51    0.05
# String.split/2                                                                      2000   1.44   52    0.03
# anonymous fn/2 in Logflare.Sources.SourceRouter.Sequential.route_with_lql_rules?/2  2000   1.44   52    0.03
# Access.get/2                                                                        2001   1.46   53    0.03
# anonymous fn/3 in Logflare.Sources.SourceRouter.Sequential.matching_rules/2         1000   1.57   57    0.06
# List.wrap/1                                                                         3000   2.13   77    0.03
# Map.get/2                                                                           3000   2.15   78    0.03
# Logflare.Sources.SourceRouter.Sequential.stringify/1                                2000   2.21   80    0.04
# anonymous fn/2 in Logflare.Sources.SourceRouter.Sequential.route_with_lql_rules?/2  2000   2.98  108    0.05
# :lists.keyfind/3                                                                    4003   3.12  113    0.03
# Map.get/3                                                                           3000   3.45  125    0.04
# Logflare.Sources.SourceRouter.Sequential.evaluate_filter_condition/2                2000   5.19  188    0.09
# :ets.lookup/2                                                                          2   5.25  190   95.00
# Keyword.get/3                                                                       4002   5.66  205    0.05
# String.split/3                                                                      2000   5.69  206    0.10
# :binary.split/3                                                                     2000   6.29  228    0.11
# Enum.predicate_list/3                                                               5000   6.54  237    0.05
# Logflare.Sources.SourceRouter.Sequential.collect_by_path/2                          5000   7.76  281    0.06
# :maps.from_keys/2                                                                      1   8.70  315  315.00
# Cachex.Actions.read/2                                                                  1   9.17  332  332.00
# :erlang.send/2                                                                         1   9.86  357  357.00

# Profile done over 105 matching functions

# Profiling GroupedRuleset with tprof...

# Profile results of #PID<0.16314.0>
# #                                                                                  CALLS      % TIME µS/CALL
# Total                                                                              78246 100.00 6309    0.08
# Kernel.>/2                                                                             1   0.00    0    0.00
# Kernel.struct!/2                                                                       1   0.00    0    0.00
# Kernel.validate_struct!/3                                                              1   0.00    0    0.00
# :erlang.integer_to_binary/1                                                            1   0.00    0    0.00
# :maps.from_list/1                                                                      1   0.00    0    0.00
# :maps.fold/3                                                                           6   0.00    0    0.00
# :maps.iterator/1                                                                       6   0.00    0    0.00
# :maps.iterator/2                                                                       6   0.00    0    0.00
# :maps.next/1                                                                           8   0.00    0    0.00
# Enum.flat_map/2                                                                        1   0.00    0    0.00
# Enum.reduce/3                                                                          5   0.00    0    0.00
# Enum.to_list/1                                                                         1   0.00    0    0.00
# anonymous fn/4 in Enum.reduce/3                                                       18   0.00    0    0.00
# Map.get/2                                                                              3   0.00    0    0.00
# Map.get/3                                                                              3   0.00    0    0.00
# Map.take/2                                                                             1   0.00    0    0.00
# :sets.from_list/2                                                                      1   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_each/3                                               1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_scenario/2                                           1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_each/2                                              1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_function/2                                          4   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_scenario/2                                          1   0.00    0    0.00
# Benchee.Benchmark.Runner.main_function/2                                               1   0.00    0    0.00
# Benchee.Benchmark.Runner.run_once/2                                                    1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.Runner.main_function/2                             1   0.00    0    0.00
# Benchee.Benchmark.BenchmarkConfig.__struct__/1                                         1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.BenchmarkConfig.__struct__/1                      13   0.00    0    0.00
# Logflare.Sources.SourceRouter.GroupedRuleset.match_cond/3                              2   0.00    0    0.00
# Logflare.Sources.SourceRouter.GroupedRuleset.matching_rule_ids/2                       1   0.00    0    0.00
# Logflare.Sources.SourceRouter.GroupedRuleset.stringify/1                               2   0.00    0    0.00
# anonymous fn/3 in Logflare.Sources.SourceRouter.GroupedRuleset.matching_rule_ids/2     2   0.00    0    0.00
# anonymous fn/3 in Logflare.Sources.SourceRouter.GroupedRuleset.match_rule/4            3   0.00    0    0.00
# anonymous fn/2 in Benchee.Profile.run/4                                                1   0.02    1    1.00
# Map.take/3                                                                            14   0.02    1    0.07
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.02    1    1.00
# Benchee.Benchmark.Runner.collect_return_value/2                                        1   0.02    1    1.00
# Logflare.Rules.Cache.ruleset_by_source_id/1                                            1   0.02    1    1.00
# Benchee.Benchmark.BenchmarkConfig.from/1                                               1   0.02    1    1.00
# Logflare.Sources.SourceRouter.GroupedRuleset.check_op/3                                2   0.02    1    0.50
# Logflare.Sources.SourceRouter.GroupedRuleset.match_rule/4                              5   0.02    1    0.20
# MapSet.new/1                                                                           1   0.03    2    2.00
# :erts_internal.map_next/3                                                              8   0.11    7    0.88
# Logflare.Sources.SourceRouter.GroupedRuleset.matching_rules/2                          1   0.13    8    8.00
# List.last/2                                                                         1001   0.43   27    0.03
# Enum.each/2                                                                         1001   0.43   27    0.03
# Cachex.Services.Informant.broadcast/3                                               1001   0.43   27    0.03
# Cachex.Router.route/3                                                               1001   0.43   27    0.03
# Cachex.Stats.async?/0                                                               1001   0.43   27    0.03
# Logflare.Rules.Cache.apply_repo_fun/2                                               1001   0.43   27    0.03
# Module.concat/2                                                                     1001   0.44   28    0.03
# anonymous fn/4 in Enum.flat_map/2                                                   1000   0.44   28    0.03
# Logflare.ContextCache.cache_name/1                                                  1001   0.44   28    0.03
# Logflare.Rules.Cache.get_rule/1                                                     1000   0.44   28    0.03
# List.last/1                                                                         1001   0.46   29    0.03
# Cachex.Services.Janitor.expired?/2                                                  1001   0.46   29    0.03
# Cachex.Services.Overseer.get/1                                                      1001   0.46   29    0.03
# Cachex.fetch/4                                                                      1001   0.46   29    0.03
# Cachex.Services.Informant.broadcast/2                                               1001   0.48   30    0.03
# Cachex.fetch/3                                                                      1001   0.48   30    0.03
# Process.get/2                                                                       1001   0.48   30    0.03
# :erlang.++/2                                                                        1000   0.49   31    0.03
# anonymous fn/4 in Cachex.fetch/4                                                    1001   0.51   32    0.03
# :maps.try_next/2                                                                    1018   0.52   33    0.03
# :elixir_aliases.to_partial/1                                                        1001   0.60   38    0.04
# :erlang.whereis/1                                                                   1001   0.67   42    0.04
# :os.system_time/1                                                                   1001   0.71   45    0.04
# Enum.flat_reverse/2                                                                 1001   0.81   51    0.05
# Cachex.Stats.type/0                                                                 1001   0.82   52    0.05
# Cachex.Services.Janitor.expired?/1                                                  1001   0.86   54    0.05
# Cachex.Stats.actions/0                                                              2002   0.86   54    0.03
# Cachex.Actions.Get.execute/3                                                        1001   0.89   56    0.06
# anonymous fn/3 in Enum.flat_map/2                                                   1000   0.92   58    0.06
# anonymous fn/2 in Cachex.Services.Informant.broadcast_action/3                      1001   0.92   58    0.06
# Access.get/2                                                                        2001   0.94   59    0.03
# Enum.filter/2                                                                       2002   0.94   59    0.03
# Cachex.Actions.Fetch.execute/4                                                      1001   0.95   60    0.06
# anonymous fn/1 in Logflare.Sources.SourceRouter.GroupedRuleset.matching_rules/2     1000   0.95   60    0.06
# :erlang.atom_to_binary/2                                                            2002   1.19   75    0.04
# :elixir_aliases.concat/1                                                            1001   1.25   79    0.08
# Cachex.Services.Overseer.with/2                                                     1001   1.33   84    0.08
# Enum."-each/2-lists^foreach/1-0-"/2                                                 2002   1.35   85    0.04
# :maps.fold_1/4                                                                      1024   1.36   86    0.08
# :erlang.binary_to_atom/2                                                            1001   1.38   87    0.09
# :lists.keyfind/3                                                                    3003   1.39   88    0.03
# Access.get/3                                                                        2001   1.41   89    0.04
# :elixir_aliases.do_concat/1                                                         2002   1.41   89    0.04
# Cachex.Services.Informant.notify/3                                                  2002   1.44   91    0.05
# Keyword.get/3                                                                       2002   1.78  112    0.06
# Cachex.Services.Informant.broadcast_action/3                                        2002   1.78  112    0.06
# :erlang.atom_to_binary/1                                                            2002   1.84  116    0.06
# Logflare.ContextCache.apply_fun/3                                                   2002   1.85  117    0.06
# anonymous fn/2 in Cachex.Services.Informant.notify/3                                1001   2.09  132    0.13
# Enum.filter_list/2                                                                  3003   2.19  138    0.05
# :elixir_aliases.do_concat/2                                                         2002   3.39  214    0.11
# Cachex.Router.route_local/3                                                         1001   3.63  229    0.23
# Cachex.Services.Overseer.retrieve/1                                                 1001   4.37  276    0.28
# :maps.from_keys/2                                                                      1   4.95  312  312.00
# Logflare.Sources.SourceRouter.GroupedRuleset.accumulate/2                           4002   5.96  376    0.09
# Cachex.Actions.read/2                                                               1001   6.53  412    0.41
# :ets.lookup/2                                                                       2002  10.08  636    0.32
# :erlang.send/2                                                                      1001  14.69  927    0.93

# Profile done over 102 matching functions

# Profiling GroupedRuleset with tprof...

# Profile results of #PID<0.16316.0>
# #                                                                                  CALLS      % TIME µS/CALL
# Total                                                                              17311 100.00 1045    0.06
# List.last/1                                                                            2   0.00    0    0.00
# List.last/2                                                                            2   0.00    0    0.00
# Cachex.Actions.read/2                                                                  2   0.00    0    0.00
# Kernel.>/2                                                                             1   0.00    0    0.00
# Kernel.struct!/2                                                                       1   0.00    0    0.00
# Kernel.validate_struct!/3                                                              1   0.00    0    0.00
# :erlang.atom_to_binary/1                                                               4   0.00    0    0.00
# :erlang.atom_to_binary/2                                                               4   0.00    0    0.00
# :erlang.binary_to_atom/2                                                               2   0.00    0    0.00
# :erlang.integer_to_binary/1                                                            1   0.00    0    0.00
# :erlang.whereis/1                                                                      2   0.00    0    0.00
# Keyword.get/3                                                                          4   0.00    0    0.00
# Access.get/2                                                                           3   0.00    0    0.00
# Access.get/3                                                                           3   0.00    0    0.00
# MapSet.new/1                                                                           1   0.00    0    0.00
# Module.concat/2                                                                        2   0.00    0    0.00
# :maps.from_list/1                                                                      1   0.00    0    0.00
# :maps.from_keys/2                                                                      1   0.00    0    0.00
# :maps.fold/3                                                                           6   0.00    0    0.00
# :maps.iterator/1                                                                       6   0.00    0    0.00
# :maps.iterator/2                                                                       6   0.00    0    0.00
# Enum.each/2                                                                            2   0.00    0    0.00
# Enum.filter/2                                                                          4   0.00    0    0.00
# Enum.filter_list/2                                                                     6   0.00    0    0.00
# Enum.flat_map/2                                                                        1   0.00    0    0.00
# Enum.reduce/3                                                                          5   0.00    0    0.00
# Enum.to_list/1                                                                         1   0.00    0    0.00
# Enum."-each/2-lists^foreach/1-0-"/2                                                    4   0.00    0    0.00
# Map.get/2                                                                              3   0.00    0    0.00
# Map.get/3                                                                              3   0.00    0    0.00
# Map.take/2                                                                             1   0.00    0    0.00
# :sets.from_list/2                                                                      1   0.00    0    0.00
# Cachex.Services.Informant.broadcast/2                                                  2   0.00    0    0.00
# Cachex.Services.Informant.broadcast/3                                                  2   0.00    0    0.00
# Cachex.Services.Informant.broadcast_action/3                                           4   0.00    0    0.00
# Cachex.Services.Informant.notify/3                                                     4   0.00    0    0.00
# anonymous fn/2 in Cachex.Services.Informant.broadcast_action/3                         2   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# Cachex.Actions.Fetch.execute/4                                                         2   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_each/3                                               1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_scenario/2                                           1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_each/2                                              1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_function/2                                          4   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_scenario/2                                          1   0.00    0    0.00
# Cachex.Services.Janitor.expired?/1                                                     2   0.00    0    0.00
# Cachex.Services.Janitor.expired?/2                                                     2   0.00    0    0.00
# Benchee.Benchmark.Runner.collect_return_value/2                                        1   0.00    0    0.00
# Benchee.Benchmark.Runner.main_function/2                                               1   0.00    0    0.00
# Benchee.Benchmark.Runner.run_once/2                                                    1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.Runner.main_function/2                             1   0.00    0    0.00
# :elixir_aliases.concat/1                                                               2   0.00    0    0.00
# :elixir_aliases.do_concat/1                                                            4   0.00    0    0.00
# :elixir_aliases.to_partial/1                                                           2   0.00    0    0.00
# Cachex.Router.route/3                                                                  2   0.00    0    0.00
# Cachex.Actions.Get.execute/3                                                           2   0.00    0    0.00
# Cachex.Services.Overseer.get/1                                                         2   0.00    0    0.00
# Cachex.Services.Overseer.retrieve/1                                                    2   0.00    0    0.00
# Cachex.Services.Overseer.with/2                                                        2   0.00    0    0.00
# Cachex.Stats.actions/0                                                                 4   0.00    0    0.00
# Cachex.Stats.async?/0                                                                  2   0.00    0    0.00
# Cachex.Stats.type/0                                                                    2   0.00    0    0.00
# Cachex.fetch/3                                                                         2   0.00    0    0.00
# Cachex.fetch/4                                                                         2   0.00    0    0.00
# anonymous fn/4 in Cachex.fetch/4                                                       2   0.00    0    0.00
# :lists.keyfind/3                                                                       6   0.00    0    0.00
# Logflare.ContextCache.apply_fun/3                                                      4   0.00    0    0.00
# Logflare.ContextCache.cache_name/1                                                     2   0.00    0    0.00
# Logflare.Rules.Cache.apply_repo_fun/2                                                  2   0.00    0    0.00
# Logflare.Rules.Cache.get_rule/1                                                        1   0.00    0    0.00
# Logflare.Rules.Cache.ruleset_by_source_id/1                                            1   0.00    0    0.00
# Benchee.Benchmark.BenchmarkConfig.__struct__/1                                         1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.BenchmarkConfig.__struct__/1                      13   0.00    0    0.00
# Process.get/2                                                                          2   0.00    0    0.00
# Logflare.Sources.SourceRouter.GroupedRuleset.matching_rule_ids/2                       1   0.00    0    0.00
# Logflare.Sources.SourceRouter.GroupedRuleset.matching_rules/2                          1   0.00    0    0.00
# anonymous fn/3 in Logflare.Sources.SourceRouter.GroupedRuleset.matching_rule_ids/2     2   0.00    0    0.00
# :maps.next/1                                                                          11   0.10    1    0.09
# anonymous fn/2 in Benchee.Profile.run/4                                                1   0.10    1    1.00
# Map.take/3                                                                            14   0.10    1    0.07
# anonymous fn/2 in Cachex.Services.Informant.notify/3                                   2   0.10    1    0.50
# :elixir_aliases.do_concat/2                                                            4   0.10    1    0.25
# Cachex.Router.route_local/3                                                            2   0.10    1    0.50
# Benchee.Benchmark.BenchmarkConfig.from/1                                               1   0.10    1    1.00
# :os.system_time/1                                                                      2   0.29    3    1.50
# :erts_internal.map_next/3                                                             11   1.05   11    1.00
# Logflare.Sources.SourceRouter.GroupedRuleset.match_cond/3                           1001   2.49   26    0.03
# Logflare.Sources.SourceRouter.GroupedRuleset.stringify/1                            1001   2.49   26    0.03
# anonymous fn/1 in Logflare.Sources.SourceRouter.GroupedRuleset.matching_rules/2     1000   2.58   27    0.03
# anonymous fn/3 in Logflare.Sources.SourceRouter.GroupedRuleset.match_rule/4         1002   2.58   27    0.03
# :erlang.++/2                                                                        1000   2.68   28    0.03
# anonymous fn/4 in Enum.flat_map/2                                                   1000   2.78   29    0.03
# :ets.lookup/2                                                                          4   3.06   32    8.00
# anonymous fn/4 in Enum.reduce/3                                                     1017   3.35   35    0.03
# Enum.flat_reverse/2                                                                 1001   4.78   50    0.05
# anonymous fn/3 in Enum.flat_map/2                                                   1000   4.88   51    0.05
# Logflare.Sources.SourceRouter.GroupedRuleset.match_rule/4                           1004   5.36   56    0.06
# :maps.try_next/2                                                                    2017   5.45   57    0.03
# :erlang.send/2                                                                         2   5.55   58   29.00
# Logflare.Sources.SourceRouter.GroupedRuleset.check_op/3                             1001   5.84   61    0.06
# :maps.fold_1/4                                                                      2023  14.83  155    0.08
# Logflare.Sources.SourceRouter.GroupedRuleset.accumulate/2                           2002  29.28  306    0.15

# Profile done over 102 matching functions

# Profiling Reference with tprof...

# Profile results of #PID<0.16318.0>
# #                                                                                  CALLS      % TIME µS/CALL
# Total                                                                              30191 100.00 2298    0.08
# List.last/1                                                                            1   0.00    0    0.00
# List.last/2                                                                            1   0.00    0    0.00
# Kernel.>/2                                                                             1   0.00    0    0.00
# Kernel.struct!/2                                                                       1   0.00    0    0.00
# Kernel.validate_struct!/3                                                              1   0.00    0    0.00
# :erts_internal.map_next/3                                                              1   0.00    0    0.00
# :erlang.atom_to_binary/1                                                               2   0.00    0    0.00
# :erlang.binary_to_atom/2                                                               1   0.00    0    0.00
# :erlang.integer_to_binary/1                                                            1   0.00    0    0.00
# :erlang.whereis/1                                                                      1   0.00    0    0.00
# MapSet.new/1                                                                           1   0.00    0    0.00
# Module.concat/2                                                                        1   0.00    0    0.00
# :maps.from_list/1                                                                      1   0.00    0    0.00
# :maps.from_keys/2                                                                      1   0.00    0    0.00
# :maps.fold/3                                                                           1   0.00    0    0.00
# :maps.iterator/1                                                                       1   0.00    0    0.00
# :maps.iterator/2                                                                       1   0.00    0    0.00
# :maps.next/1                                                                           1   0.00    0    0.00
# :maps.try_next/2                                                                      13   0.00    0    0.00
# Enum.each/2                                                                            1   0.00    0    0.00
# Enum.filter/2                                                                          2   0.00    0    0.00
# Enum.filter_list/2                                                                     3   0.00    0    0.00
# Enum.reduce/3                                                                          2   0.00    0    0.00
# Enum.to_list/1                                                                         1   0.00    0    0.00
# anonymous fn/4 in Enum.reduce/3                                                       13   0.00    0    0.00
# Enum."-each/2-lists^foreach/1-0-"/2                                                    2   0.00    0    0.00
# Map.take/2                                                                             1   0.00    0    0.00
# :sets.from_list/2                                                                      1   0.00    0    0.00
# Cachex.Services.Informant.broadcast/2                                                  1   0.00    0    0.00
# Cachex.Services.Informant.broadcast/3                                                  1   0.00    0    0.00
# Cachex.Services.Informant.broadcast_action/3                                           2   0.00    0    0.00
# Cachex.Services.Informant.notify/3                                                     2   0.00    0    0.00
# anonymous fn/2 in Cachex.Services.Informant.broadcast_action/3                         1   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# anonymous fn/1 in :elixir_compiler_16.__FILE__/1                                       1   0.00    0    0.00
# Cachex.Actions.Fetch.execute/4                                                         1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_each/3                                               1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_after_scenario/2                                           1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_each/2                                              1   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_function/2                                          4   0.00    0    0.00
# Benchee.Benchmark.Hooks.run_before_scenario/2                                          1   0.00    0    0.00
# Cachex.Services.Janitor.expired?/1                                                     1   0.00    0    0.00
# Cachex.Services.Janitor.expired?/2                                                     1   0.00    0    0.00
# Benchee.Benchmark.Runner.collect_return_value/2                                        1   0.00    0    0.00
# Benchee.Benchmark.Runner.main_function/2                                               1   0.00    0    0.00
# Benchee.Benchmark.Runner.run_once/2                                                    1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.Runner.main_function/2                             1   0.00    0    0.00
# :elixir_aliases.concat/1                                                               1   0.00    0    0.00
# :elixir_aliases.do_concat/1                                                            2   0.00    0    0.00
# :elixir_aliases.to_partial/1                                                           1   0.00    0    0.00
# Cachex.Router.route/3                                                                  1   0.00    0    0.00
# :os.system_time/1                                                                      1   0.00    0    0.00
# Cachex.Actions.Get.execute/3                                                           1   0.00    0    0.00
# Cachex.Services.Overseer.get/1                                                         1   0.00    0    0.00
# Cachex.Services.Overseer.retrieve/1                                                    1   0.00    0    0.00
# Cachex.Services.Overseer.with/2                                                        1   0.00    0    0.00
# Cachex.Stats.actions/0                                                                 2   0.00    0    0.00
# Cachex.Stats.async?/0                                                                  1   0.00    0    0.00
# Cachex.Stats.type/0                                                                    1   0.00    0    0.00
# Cachex.fetch/3                                                                         1   0.00    0    0.00
# Cachex.fetch/4                                                                         1   0.00    0    0.00
# anonymous fn/4 in Cachex.fetch/4                                                       1   0.00    0    0.00
# :lists.reverse/1                                                                       1   0.00    0    0.00
# Logflare.ContextCache.apply_fun/3                                                      2   0.00    0    0.00
# Logflare.ContextCache.cache_name/1                                                     1   0.00    0    0.00
# Logflare.Rules.Cache.apply_repo_fun/2                                                  1   0.00    0    0.00
# Logflare.Rules.Cache.list_by_source_id/1                                               1   0.00    0    0.00
# Logflare.Rules.Cache.list_rules/1                                                      1   0.00    0    0.00
# Benchee.Benchmark.BenchmarkConfig.__struct__/1                                         1   0.00    0    0.00
# Benchee.Benchmark.BenchmarkConfig.from/1                                               1   0.00    0    0.00
# anonymous fn/2 in Benchee.Benchmark.BenchmarkConfig.__struct__/1                      13   0.00    0    0.00
# Process.get/2                                                                          1   0.00    0    0.00
# :erlang.atom_to_binary/2                                                               2   0.04    1    0.50
# :maps.fold_1/4                                                                        14   0.04    1    0.07
# anonymous fn/2 in Benchee.Profile.run/4                                                1   0.04    1    1.00
# Map.take/3                                                                            14   0.04    1    0.07
# anonymous fn/2 in Cachex.Services.Informant.notify/3                                   1   0.04    1    1.00
# Logflare.Sources.SourceRouter.Sequential.matching_rules/2                              1   0.04    1    1.00
# :elixir_aliases.do_concat/2                                                            2   0.04    1    0.50
# Cachex.Router.route_local/3                                                            1   0.04    1    1.00
# Access.get/3                                                                        1002   1.13   26    0.03
# Enum.any?/2                                                                         1001   1.13   26    0.03
# Access.get/2                                                                        1002   1.17   27    0.03
# Enum.all?/2                                                                         1000   1.17   27    0.03
# Logflare.Sources.SourceRouter.Sequential.route_with_lql_rules?/2                    1000   1.17   27    0.03
# anonymous fn/2 in Logflare.Sources.SourceRouter.Sequential.route_with_lql_rules?/2  1001   1.17   27    0.03
# String.split/2                                                                      1001   1.22   28    0.03
# Logflare.Sources.SourceRouter.Sequential.stringify/1                                1001   1.22   28    0.03
# Enum."-reduce/3-lists^foldl/2-0-"/3                                                 1001   2.26   52    0.05
# List.wrap/1                                                                         2001   2.44   56    0.03
# Map.get/2                                                                           2001   2.44   56    0.03
# :lists.keyfind/3                                                                    2005   2.48   57    0.03
# anonymous fn/3 in Logflare.Sources.SourceRouter.Sequential.matching_rules/2         1000   2.79   64    0.06
# anonymous fn/2 in Logflare.Sources.SourceRouter.Sequential.route_with_lql_rules?/2  1001   2.87   66    0.07
# Map.get/3                                                                           2001   3.35   77    0.04
# Logflare.Sources.SourceRouter.Sequential.evaluate_filter_condition/2                1001   3.61   83    0.08
# Keyword.get/3                                                                       2004   4.66  107    0.05
# String.split/3                                                                      1001   4.83  111    0.11
# Enum.predicate_list/3                                                               3002   6.09  140    0.05
# :ets.lookup/2                                                                          2   6.14  141   70.50
# :binary.split/3                                                                     1001   6.61  152    0.15
# Logflare.Sources.SourceRouter.Sequential.collect_by_path/2                          3002   8.05  185    0.06
# Cachex.Actions.read/2                                                                  1  14.58  335  335.00
# :erlang.send/2                                                                         1  17.06  392  392.00
