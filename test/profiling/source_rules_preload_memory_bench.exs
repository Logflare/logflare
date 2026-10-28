# credo:disable-for-this-file Credo.Check.Refactor.IoPuts
#
# Usage: MIX_ENV=test mix run test/profiling/source_rules_preload_memory_bench.exs
#
# Measures the memory impact of no longer preloading `rules` onto `%Source{}` on the
# ingest path. Each section compares the removed behaviour ("before") against the
# current one ("after") for sources with 0, 10, 100 and 1000 rules:
#
#   1. Per-request lookup: Sources.Cache.get_by_for_ingest/1, used by FetchResource,
#      LogChannel, the gRPC VerifyApiResourceAccess interceptor and UserMonitoring.
#   2. SourceSup tree: SourceSup.init/1 hands the source to every child, so each child
#      process held its own copy of the rules list. Measured as the flat size of each
#      child's start args, which is what spawning copies into the child's heap. This is a
#      lower bound: it excludes SourceSupWorker (not started in the test env) and any
#      grandchildren a backend passes the source on to. Starting the real tree is avoided
#      because its children call BigQuery.
#   3. SingleTenant.update_supabase_source_schemas/0: one-off Repo.preload(:rules) over
#      all of the default user's sources.

alias Logflare.Backends.SourceSup
alias Logflare.Repo
alias Logflare.Rules
alias Logflare.Sources

import Logflare.Factory

Ecto.Adapters.SQL.Sandbox.start_owner!(Repo, shared: true, ownership_timeout: 1_200_000)

insert(:plan)
user = insert(:user)
rule_counts = [0, 10, 100, 1000]

preload_rules_from_cache = fn source ->
  Repo.preload(source, rules: fn [id] -> Rules.Cache.list_by_source_id(id) end)
end

sources =
  Map.new(rule_counts, fn n ->
    source = insert(:source, user: user)
    backend = insert(:backend, user: user)

    for i <- 1..n//1 do
      insert(:rule, source: source, backend: backend, lql_string: "project-#{i}")
    end

    {n, source}
  end)

words_to_kb = fn words -> Float.round(words * :erlang.system_info(:wordsize) / 1024, 2) end

IO.puts("\n== Retained size of the %Source{} struct ==\n")
IO.puts("rules | before (KB) | after (KB) | delta (KB)")

for n <- rule_counts do
  source = Sources.Cache.get_by_for_ingest(token: sources[n].token)
  before = :erts_debug.size(preload_rules_from_cache.(source))
  after_ = :erts_debug.size(source)

  IO.puts(
    "#{String.pad_leading("#{n}", 5)} | #{String.pad_leading("#{words_to_kb.(before)}", 11)} | " <>
      "#{String.pad_leading("#{words_to_kb.(after_)}", 10)} | #{words_to_kb.(before - after_)}"
  )
end

IO.puts("\n== 1. Per-request lookup (allocations per call) ==\n")

Benchee.run(
  %{
    "before: get_by_for_ingest + preload rules" => fn token ->
      [token: token] |> Sources.Cache.get_by_for_ingest() |> preload_rules_from_cache.()
    end,
    "after: get_by_for_ingest" => fn token -> Sources.Cache.get_by_for_ingest(token: token) end
  },
  inputs: Map.new(rule_counts, fn n -> {"#{n} rules", sources[n].token} end),
  before_scenario: fn token ->
    Sources.Cache.get_by_for_ingest(token: token)
    |> preload_rules_from_cache.()

    token
  end,
  time: 2,
  warmup: 1,
  memory_time: 1
)

child_args_kb = fn source ->
  {:ok, {_flags, specs}} = SourceSup.init(source)
  words = specs |> Enum.map(&:erts_debug.flat_size(&1.start)) |> Enum.sum()
  {length(specs), words_to_kb.(words)}
end

IO.puts("\n== 2. SourceSup.init/1 child start args (copied into each child's heap) ==\n")
IO.puts("rules | children | before (KB) | after (KB) | delta (KB)")

for n <- rule_counts do
  source = Sources.Cache.get_by_id(sources[n].id)
  {_, before} = child_args_kb.(preload_rules_from_cache.(source))
  {children, after_} = child_args_kb.(source)

  IO.puts(
    "#{String.pad_leading("#{n}", 5)} | #{String.pad_leading("#{children}", 8)} | " <>
      "#{String.pad_leading("#{before}", 11)} | #{String.pad_leading("#{after_}", 10)} | " <>
      "#{Float.round(before - after_, 2)}"
  )
end

IO.puts("\n== 3. SingleTenant.update_supabase_source_schemas/0 source listing ==\n")

Benchee.run(
  %{
    "before: list_sources_by_user + Repo.preload(:rules)" => fn user ->
      user |> Sources.list_sources_by_user() |> Repo.preload(:rules)
    end,
    "after: list_sources_by_user" => fn user -> Sources.list_sources_by_user(user) end
  },
  inputs: %{"#{map_size(sources)} sources, #{Enum.sum(rule_counts)} rules" => user},
  time: 2,
  warmup: 1,
  memory_time: 1
)
