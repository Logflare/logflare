alias Logflare.Sources
import Logflare.Factory

# Setup test data
user = insert(:user)

Benchee.run(
  %{
    "bust_keys with ETS filter" => fn [source | _] ->
      ContextCache.bust_keys([{Sources, source.id}])
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
  inputs: %{"1k sources" => insert_list(1000, :source, user: user)},
  before_each: fn sources ->
    Cachex.clear(Sources.Cache)
    # Populate cache with test data
    Enum.each(sources, fn source ->
      cache_key = {:get_by, [[token: source.token]]}
      Cachex.put!(Sources.Cache, cache_key, {:cached, source})
      cache_key = {:get_by, [[id: source.id]]}
      Cachex.put!(Sources.Cache, cache_key, {:cached, source})
      cache_key = {:get_by_and_preload, [[token: source.token]]}
      Cachex.put!(Sources.Cache, cache_key, {:cached, source})
    end)

    sources
  end,
  pre_check: :all_same,
  time: 4,
  memory_time: 2
)
