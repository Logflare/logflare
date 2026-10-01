defmodule Logflare.CacheTest do
  use ExUnit.Case, async: true

  alias Logflare.Cache.CachexOps

  defmodule TestCache do
    use Logflare.Cache
  end

  defmodule NotStartedCache do
    use Logflare.Cache
  end

  defmodule RecordingImpl do
    @behaviour Logflare.Cache.Ops

    @impl true
    def healthy?(cache), do: record({:healthy?, cache}, true)
    @impl true
    def stats(cache), do: record({:stats, cache}, %{})
    @impl true
    def reset(cache), do: record({:reset, cache}, :ok)

    defp record(call, result) do
      send(self(), call)
      result
    end
  end

  defmodule CustomImplCache do
    use Logflare.Cache, impl: RecordingImpl
  end

  setup do
    start_supervised!(CachexOps.child_spec(TestCache, limit: 100))
    :ok
  end

  test "started cache" do
    assert TestCache.healthy?()
  end

  test "cache that is not started" do
    refute NotStartedCache.healthy?()
  end

  test "cache after reads and writes" do
    Cachex.put(TestCache, :key, :value)
    Cachex.get(TestCache, :key)
    Cachex.get(TestCache, :missing)

    assert %{hits: 1, misses: 1, total_heap_size: heap} = stats = TestCache.stats()
    assert heap > 0

    assert stats |> Map.keys() |> Enum.sort() == [
             :evictions,
             :expirations,
             :hit_rate,
             :hits,
             :miss_rate,
             :misses,
             :operations,
             :total_heap_size
           ]
  end

  test "cache with entries and stats, after reset" do
    Cachex.put(TestCache, :key, :value)
    Cachex.get(TestCache, :key)

    assert :ok = TestCache.reset()

    assert {:ok, nil} = Cachex.get(TestCache, :key)
    assert %{hits: 0} = TestCache.stats()
  end

  test "cache with a custom impl module" do
    assert CustomImplCache.healthy?()
    assert %{} = CustomImplCache.stats()
    assert :ok = CustomImplCache.reset()

    assert_received {:healthy?, CustomImplCache}
    assert_received {:stats, CustomImplCache}
    assert_received {:reset, CustomImplCache}
  end
end
