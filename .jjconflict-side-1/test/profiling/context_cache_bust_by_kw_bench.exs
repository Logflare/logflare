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

Benchee.run(
  %{
    "bust_keys by primary key" => fn [{_source, rule} | _] ->
      pkey = rule.id
      ContextCache.bust_keys([{Rules, pkey}])
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

##### With input 1K sources with 20 rules #####
# Name                                ips        average  deviation         median         99th %
# bust_keys by relation key      207.70 K     0.00481 ms    ±67.05%     0.00396 ms      0.0247 ms
# bust_keys by primary key        0.186 K        5.36 ms     ±8.86%        5.26 ms        8.34 ms

# Comparison:
# bust_keys by relation key      207.70 K
# bust_keys by primary key        0.186 K - 1113.70x slower +5.36 ms

# Memory usage statistics:

# Name                         Memory usage
# bust_keys by relation key       0.0229 MB
# bust_keys by primary key         19.18 MB - 839.47x memory usage +19.16 MB
