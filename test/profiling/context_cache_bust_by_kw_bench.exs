alias Logflare.Sources
alias Logflare.Rules
alias Logflare.Users
import Logflare.Factory
# Setup test data
user = insert(:user)

cache = Rules.Cache

Benchee.run(
  %{
    "bust_keys with ETS filter" => fn [{_source, rule} | _] ->
      pkey = rule.id

      filter =
        {
          # use orelse to prevent 2nd condition failing as value is not a map
          :orelse,
          {
            :orelse,
            # handle lists
            {:is_list, {:element, 2, :value}},
            # handle :ok tuples when struct with id is in 2nd element pos.
            {:andalso, {:is_tuple, {:element, 2, :value}},
             {:andalso, {:==, {:element, 1, {:element, 2, :value}}, :ok},
              {:andalso, {:is_map, {:element, 2, {:element, 2, :value}}},
               {:==, {:map_get, :id, {:element, 2, {:element, 2, :value}}}, pkey}}}}
          },
          # handle single maps
          {:andalso, {:is_map, {:element, 2, :value}},
           {:==, {:map_get, :id, {:element, 2, :value}}, pkey}}
        }

      query = Cachex.Query.build(where: filter, output: {:key, :value})

      cache
      |> Cachex.stream!(query)
      |> Stream.filter(fn
        {_k, {:cached, v}} when is_list(v) ->
          Enum.any?(v, &(&1.id == rule.id))

        {_k, _v} ->
          true
      end)
      |> Enum.reduce(0, fn {k, _v}, acc ->
        Cachex.del(cache, k)
        acc + 1
      end)
    end,
    "bust_keys by key with Cachex.execute" => fn [{source, _rule} | _] ->
      [source_id: source.id]
      |> Enum.map(fn
        {:source_id, source_id} -> {:list_by_source_id, [source_id]}
        {:backend_id, backend_id} -> {:list_by_backend_id, [backend_id]}
      end)
      |> then(fn entries ->
        Cachex.execute!(cache, fn worker ->
          Enum.reduce(entries, 0, fn {k, _v}, acc ->
            case Cachex.take(worker, k) do
              {:ok, nil} -> acc
              {:ok, _value} -> acc + 1
            end
          end)
        end)
      end)
    end,
    "bust_keys by key" => fn [{source, _rule} | _] ->
      [source_id: source.id]
      |> Enum.map(fn
        {:source_id, source_id} -> {:list_by_source_id, [source_id]}
        {:backend_id, backend_id} -> {:list_by_backend_id, [backend_id]}
      end)
      |> then(fn entries ->
        Cachex.execute!(cache, fn worker ->
          Enum.reduce(entries, 0, fn {k, _v}, acc ->
            case Cachex.take(worker, k) do
              {:ok, nil} -> acc
              {:ok, _value} -> acc + 1
            end
          end)
        end)
      end)
    end
  },
  before_each: fn _input ->
    Cachex.clear(cache)
    # Populate cache with test data
    sources =
      for i <- 1..1000 do
        source = insert(:source, user: user)
        cache_key = {:list_by_source_id, [source.id]}

        # rules = insert_list(50, :rule, source: source)
        rules = insert_list(20, :rule, source: source)

        Cachex.put!(cache, cache_key, {:cached, rules})
        {source, Enum.at(rules, 10)}
      end

    sources
  end,
  time: 4,
  memory_time: 2
)

# benchmarked on 2025-12-10 for 50 rules - exec took 11 minutes
# Name                                           ips        average  deviation         median         99th %
# bust_keys by key                          228.57 K        4.38 μs     ±0.00%        4.38 μs        4.38 μs
# bust_keys by key with Cachex.execute      154.82 K        6.46 μs     ±0.00%        6.46 μs        6.46 μs
# bust_keys with ETS filter                   2.68 K      372.67 μs     ±0.00%      372.67 μs      372.67 μs

# Comparison:
# bust_keys by key                          228.57 K
# bust_keys by key with Cachex.execute      154.82 K - 1.48x slower +2.08 μs
# bust_keys with ETS filter                   2.68 K - 85.18x slower +368.29 μs

# Memory usage statistics:

# Name                                    Memory usage
# bust_keys by key                               896 B
# bust_keys by key with Cachex.execute           896 B - 1.00x memory usage +0 B
# bust_keys with ETS filter                   994224 B - 1109.63x memory usage +993328 B
