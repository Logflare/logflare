alias Logflare.Sources
import Logflare.Factory
# Setup test data
user = insert(:user)

Benchee.run(
  %{
    "bust_keys with ETS filter" => fn [source | _] = sources ->
      pkey = source.id

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

      Sources.Cache
      |> Cachex.stream!(query)
      |> Stream.filter(fn
        {_k, {:cached, v}} when is_list(v) ->
          Enum.any?(v, &(&1.id == pkey))

        {_k, _v} ->
          true
      end)
      |> Enum.reduce(0, fn {k, _v}, acc ->
        Cachex.del(Sources.Cache, k)
        acc + 1
      end)
    end,
    "bust_keys with ETS filter under Cachex.execute" => fn [source | _] = sources ->
      pkey = source.id

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

      Sources.Cache
      |> Cachex.stream!(query)
      |> Stream.filter(fn
        {_k, {:cached, v}} when is_list(v) ->
          Enum.any?(v, &(&1.id == pkey))

        {_k, _v} ->
          true
      end)
      |> then(fn entries ->
        Cachex.execute!(Sources.Cache, fn worker ->
          Enum.reduce(entries, 0, fn {k, _v}, acc ->
            Cachex.del(worker, k)
            acc + 1
          end)
        end)
      end)
    end,
    "bust_keys with Elixir match" => fn [source | _] = sources ->
      Sources.Cache
      |> Cachex.stream!(Cachex.Query.build(output: {:key, :value}))
      |> Stream.filter(fn
        {_k, {:cached, v}} when is_list(v) ->
          Enum.any?(v, &(&1.id == source.id))

        {_k, {:cached, %{id: id}}} when id == source.id ->
          true

        {_k, {:cached, {:ok, %{id: id}}}} when id == source.id ->
          true

        _ ->
          false
      end)
      |> Enum.reduce(0, fn {k, _v}, acc ->
        Cachex.del(Sources.Cache, k)
        acc + 1
      end)
    end
  },
  before_each: fn _input ->
    Cachex.clear(Sources.Cache)
    # Populate cache with test data
    sources =
      for i <- 1..1000 do
        source = insert(:source, user: user)
        cache_key = {:get_by, [[token: source.token]]}
        Cachex.put!(Sources.Cache, cache_key, {:cached, source})
        cache_key = {:get_by, [[id: source.id]]}
        Cachex.put!(Sources.Cache, cache_key, {:cached, source})
        cache_key = {:get_by_and_preload, [[token: source.token]]}
        Cachex.put!(Sources.Cache, cache_key, {:cached, source})
        source
      end

    sources
  end,
  time: 4,
  memory_time: 2
)

# benchmarked on 2025-12-10 with Cachex.del calls & more keys
# Name                                                     ips        average  deviation         median         99th %
# bust_keys with ETS filter under Cachex.execute        3.93 K      254.57 μs    ±17.35%      244.17 μs         303 μs
# bust_keys with ETS filter                             2.46 K      406.89 μs    ±34.39%      397.46 μs      551.29 μs
# bust_keys with Elixir match                          0.129 K     7768.36 μs     ±8.64%     7644.92 μs     8492.50 μs

# Comparison:
# bust_keys with ETS filter under Cachex.execute        3.93 K
# bust_keys with ETS filter                             2.46 K - 1.60x slower +152.32 μs
# bust_keys with Elixir match                          0.129 K - 30.52x slower +7513.79 μs

# Memory usage statistics:

# Name                                                   average  deviation         median         99th %
# bust_keys with ETS filter under Cachex.execute        23.27 KB     ±0.00%       23.27 KB       23.27 KB
# bust_keys with ETS filter                             24.01 KB     ±0.00%       24.01 KB       24.01 KB
# bust_keys with Elixir match                        15231.11 KB     ±0.23%    15231.11 KB    15255.59 KB

# Comparison:
# bust_keys with ETS filter under Cachex.execute        23.27 KB
# bust_keys with ETS filter                             24.01 KB - 1.03x memory usage +0.74 KB
# bust_keys with Elixir match                        15231.11 KB - 654.66x memory usage +15207.85 KB

# benchmarked on 2025-03-03
# Name                                  ips        average  deviation         median         99th %
# bust_keys with ETS filter          7.00 K       0.143 ms     ±0.00%       0.143 ms       0.143 ms
# bust_keys with Elixir match        0.41 K        2.44 ms     ±0.00%        2.44 ms        2.44 ms

# Comparison:
# bust_keys with ETS filter          7.00 K
# bust_keys with Elixir match        0.41 K - 17.07x slower +2.30 ms

# Memory usage statistics:

# Name                           Memory usage
# bust_keys with ETS filter        0.00986 MB
# bust_keys with Elixir match         4.94 MB - 500.72x memory usage +4.93 MB
