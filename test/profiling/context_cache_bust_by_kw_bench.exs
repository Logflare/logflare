alias Logflare.ContextCache
alias Logflare.Rules

import Logflare.Factory

# Setup test data
user = insert(:user)

cache = Rules.Cache

make_input = fn sources_num, rules_num ->
  for _i <- 1..sources_num do
    rules = build_list(rules_num, :rule)
    source = insert(:source, user: user, rules: rules)
    source
  end
end

defmodule FakeRules.Cache do
  @behaviour Logflare.ContextCache

  @impl true
  def bust_by(kw) do
    kw
    |> Enum.map(fn
      {:source_id, source_id} -> {:list_by_source_id, [source_id]}
      {:backend_id, backend_id} -> {:list_by_backend_id, [backend_id]}
    end)
    |> then(fn entries ->
      Cachex.execute(Rules.Cache, fn worker ->
        Enum.reduce(entries, 0, fn k, acc ->
          case Cachex.take(worker, k) do
            {:ok, nil} -> acc
            {:ok, _value} -> acc + 1
          end
        end)
      end)
    end)
  end
end

Benchee.run(
  %{
    "bust_keys by primary key" => fn [{_source, rule} | _] ->
      pkey = rule.id
      ContextCache.bust_keys([{Rules, pkey}])
    end,
    "bust_keys by relation key with Cachex.execute" => fn [{source, _rule} | _] ->
      ContextCache.bust_keys([{FakeRules, source_id: source.id}])
    end,
    "bust_keys by relation key" => fn [{source, _rule} | _] ->
      ContextCache.bust_keys([{Rules, source_id: source.id}])
    end
  },
  before_each: fn sources ->
    Cachex.execute!(cache, fn worker ->
      Cachex.clear(worker)
      # Populate cache with test data
      for source <- sources do
        cache_key = {:list_by_source_id, [source.id]}
        Cachex.put!(worker, cache_key, {:cached, source.rules})
        {source, Enum.at(source.rules, 10)}
      end
    end)
  end,
  inputs: %{"1K sources with 20 rules" => make_input.(1000, 20)},
  pre_check: :all_same,
  time: 4,
  memory_time: 2
)
