alias Logflare.Sources
import Logflare.Factory

# Setup test data
user = insert(:user)

Benchee.run(
  %{
    "bust_keys with ETS filter" => fn [source | _] ->
      ContextCache.bust_keys([{Sources, source.id}])
    end
  },
  inputs: %{"1k sources" => insert_list(1000, :source, user: user)},
  before_each: fn sources ->
    Cachex.clear(Sources.Cache)
    # Populate cache with test data
    Cachex.execute!(Sources.Cache, fn worker ->
      Enum.each(sources, fn source ->
        cache_key = {:get_by, [[token: source.token]]}
        Cachex.put!(worker, cache_key, {:cached, source})
        cache_key = {:get_by, [[id: source.id]]}
        Cachex.put!(worker, cache_key, {:cached, source})
        cache_key = {:get_by_and_preload, [[token: source.token]]}
        Cachex.put!(worker, cache_key, {:cached, source})
      end)
    end)

    sources
  end,
  pre_check: :all_same,
  time: 4,
  memory_time: 2
)
