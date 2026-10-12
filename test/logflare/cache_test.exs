defmodule Logflare.CacheTest do
  use ExUnit.Case, async: true

  alias Logflare.Cache
  alias Logflare.Cache.CachexOps

  defmodule DefaultCache do
    @behaviour Logflare.Cache
  end

  defmodule CustomCache do
    @behaviour Logflare.Cache

    @impl true
    def healthy?, do: false
    @impl true
    def stats, do: %{hits: 42}
    @impl true
    def reset, do: send(self(), :reset) && :ok
  end

  test "cache without callbacks" do
    start_supervised!(CachexOps.child_spec(DefaultCache, limit: nil))
    Cachex.put(DefaultCache, :key, :value)
    Cachex.get(DefaultCache, :key)

    assert Cache.healthy?(DefaultCache)
    assert %{hits: 1} = Cache.stats(DefaultCache)
    assert :ok = Cache.reset(DefaultCache)
    assert {:ok, 0} = Cachex.size(DefaultCache)
  end

  test "cache implementing the callbacks" do
    refute Cache.healthy?(CustomCache)
    assert %{hits: 42} = Cache.stats(CustomCache)
    assert :ok = Cache.reset(CustomCache)
    assert_received :reset
  end
end
