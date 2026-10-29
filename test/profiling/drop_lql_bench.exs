# Usage: MIX_ENV=test mix run test/profiling/drop_lql_bench.exs

alias Logflare.LogEvent
alias Logflare.Lql.Parser
alias Logflare.Rules.Rule
alias Logflare.Sources.SourceRouter.Sequential

body = %{
  "id" => "abc",
  "timestamp" => 1_765_540_359_181_556,
  "event_message" => "GET /api/v1/resource 503 request timeout upstream",
  "metadata" => %{
    "level" => "error",
    "status" => 503,
    "user_id" => "user-123",
    "request" => %{"method" => "GET", "path" => "/api/v1/resource"},
    "tags" => ["a", "b", "c", "d", "e"],
    "spans" => for(i <- 1..10, do: %{"name" => "span-#{i}", "duration" => i * 10})
  }
}

events = for i <- 1..250, do: %LogEvent{id: "id-#{i}", body: body}

rule = fn lql ->
  {:ok, filters} = Parser.parse(lql)
  %Rule{lql_filters: filters}
end

Benchee.run(
  %{
    "route_with_lql_rules? x250" => fn rule ->
      Enum.each(events, &Sequential.route_with_lql_rules?(&1, rule))
    end
  },
  inputs: %{
    "eq + comparison" => rule.("m.level:error m.status:>500"),
    "regex on message + nested path" => rule.("~timeout m.request.path:~\"api/v[0-9]\""),
    "list regex + nested list eq" => rule.("m.tags:@>~z m.spans.name:span-10")
  },
  time: 4,
  warmup: 1,
  memory_time: 1,
  reduction_time: 1
)
