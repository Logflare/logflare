alias Logflare.Sources
alias Logflare.Users
import Logflare.Factory
# Setup test data
user = insert(:user)
source = insert(:source, user: user)

# Populate cache with test data
for i <- 1..1000 do
  cache_key = {:get_by, [[token: "token_#{i}"]]}
  Cachex.put!(Sources.Cache, cache_key, {:cached, %{id: i, token: "token_#{i}"}})
end

# Add our target record
cache_key = {:get_by, [[token: source.token]]}
Cachex.put!(Sources.Cache, cache_key, {:cached, source})

filter = {
  :orelse,
  {
    :orelse,
    {:is_list, {:element, 2, :value}},
    {:andalso, {:is_tuple, {:element, 2, :value}},
     {:andalso, {:==, {:element, 1, {:element, 2, :value}}, :ok},
      {:andalso, {:is_map, {:element, 2, {:element, 2, :value}}},
       {:==, {:map_get, :id, {:element, 2, {:element, 2, :value}}}, source.id}}}}
  },
  {:andalso, {:is_map, {:element, 2, :value}},
   {:==, {:map_get, :id, {:element, 2, :value}}, source.id}}
}

query = Cachex.Query.build(where: filter, output: {:key, :value})

Benchee.run(
  %{
    "bust_keys with ETS filter" => fn ->
      Sources.Cache
      |> Cachex.stream!(query)
      |> Enum.reduce(0, fn
        {k, {:cached, v}}, acc when is_list(v) ->
          if Enum.any?(v, fn %{id: id} -> id == source.id end) do
            acc + 1
          else
            acc
          end

        {_k, _v}, acc ->
          acc + 1
      end)
    end,
    "bust_keys with Elixir match" => fn ->
      Sources.Cache
      |> Cachex.stream!(Cachex.Query.build(output: {:key, :value}))
      |> Stream.filter(fn {_k, {:cached, v}} ->
        cond do
          is_list(v) -> Enum.any?(v, &(&1.id == source.id))
          is_map(v) -> Map.get(v, :id) == source.id
          {:ok, %{id: id}} = v -> id == source.id
          true -> false
        end
      end)
      |> Enum.count()
    end
  },
  before_each: fn input ->
    Cachex.clear(Sources.Cache)
    # Repopulate cache
    for i <- 1..1000 do
      cache_key = {:get_by, [[token: "token_#{i}"]]}
      Cachex.put!(Sources.Cache, cache_key, {:cached, %{id: i, token: "token_#{i}"}})
    end
    Cachex.put!(Sources.Cache, cache_key, {:cached, source})
    input
  end,
  time: 4,
  memory_time: 2
)

# Name                                  ips        average  deviation         median         99th %
# bust_keys with ETS filter         10.23 K       97.75 μs    ±36.05%       94.79 μs      137.00 μs
# bust_keys with Elixir match        4.84 K      206.63 μs     ±7.33%      202.79 μs      272.72 μs

# Comparison:
# bust_keys with ETS filter         10.23 K
# bust_keys with Elixir match        4.84 K - 2.11x slower +108.88 μs

# Memory usage statistics:

# Name                           Memory usage
# bust_keys with ETS filter           1.48 KB
# bust_keys with Elixir match       255.24 KB - 171.95x memory usage +253.76 KB
