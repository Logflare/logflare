alias Logflare.Logs.GroupedSourceRouting
alias Logflare.Logs.SourceRouting
alias Logflare.Backends.SourceSup
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

make_input = fn rules_num ->
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
          }
        ],
        backend_id: backend.id
      }
    end

  source = insert(:source, user: user, rules: rules)

  log_event =
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
      source_id: source.id
    )

  {log_event, source}
end

Benchee.run(
  %{
    "Reference" =>
      {fn {event, source} ->
         SourceRouting.matching_rules(event, source) |> Enum.map(& &1.id)
       end,
       before_scenario: fn {event, source} ->
         # warmup cache
         _rules = Rules.Cache.list_rules(source)
         {event, source}
       end},
    "Grouped" =>
      {fn {event, source} ->
         rule_set = RuleSet.make(source.rules)
         GroupedSourceRouting.matching_rules(event, rule_set)
       end,
       before_scenario: fn {event, source} ->
         source
         |> Ecto.reset_fields([:rules])
         |> Sources.Cache.preload_rules()

         {event, source}
       end},
    "Grouped cached" =>
      {fn {event, source} ->
         rule_set = Rules.Cache.ruleset_by_source_id(source.id)
         GroupedSourceRouting.matching_rules(event, rule_set)
       end,
       before_scenario: fn {event, source} ->
         _rule_set = Rules.Cache.ruleset_by_source_id(source.id)
         {event, source}
       end}
  },
  inputs: %{"event and source with 100 rules" => make_input.(100)},
  pre_check: :all_same,
  # profile_after: :tprof,
  warmup: 5,
  time: 5,
  memory_time: 1
)

Ecto.Adapters.SQL.Sandbox.stop_owner(pid)
