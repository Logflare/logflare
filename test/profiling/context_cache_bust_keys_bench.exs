alias Logflare.Sources
alias Logflare.Users
import Logflare.Factory
# Setup test data
user = insert(:user)

Benchee.run(
  %{
    "bust_keys with ETS filter" => fn [source | _] = sources ->
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
      |> Enum.count()
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
        source
      end

    sources
  end,
  time: 4,
  memory_time: 2
)

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
