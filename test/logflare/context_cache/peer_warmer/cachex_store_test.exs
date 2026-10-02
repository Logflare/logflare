defmodule Logflare.ContextCache.PeerWarmer.CachexStoreTest do
  use ExUnit.Case, async: true

  alias Logflare.ContextCache.PeerWarmer.CachexStore

  @source :cachex_store_test_source
  @target :cachex_store_test_target

  setup do
    for cache <- [@source, @target] do
      start_supervised!(Supervisor.child_spec({Cachex, [cache, []]}, id: cache))
    end

    :ok
  end

  test "copies entries between caches keeping their remaining TTL" do
    Cachex.put!(@source, :with_ttl, "a", expire: to_timeout(minute: 10))
    Cachex.put!(@source, :without_ttl, "b")

    entries = @source |> CachexStore.stream() |> Enum.to_list()

    assert entries |> Enum.map(&CachexStore.key/1) |> Enum.sort() == [:with_ttl, :without_ttl]
    assert entries |> Enum.map(&CachexStore.value/1) |> Enum.sort() == ["a", "b"]

    assert :ok = CachexStore.put_entries(@target, entries)

    assert CachexStore.size(@target) == 2
    assert CachexStore.exists?(@target, :with_ttl)
    refute CachexStore.exists?(@target, :missing)
    assert {:ok, ttl} = Cachex.ttl(@target, :with_ttl)
    assert ttl > to_timeout(minute: 9)
    assert {:ok, nil} = Cachex.ttl(@target, :without_ttl)
  end

  test "does not stream expired entries" do
    Cachex.put!(@source, :expired, "a", expire: 1)
    Process.sleep(5)

    assert @source |> CachexStore.stream() |> Enum.to_list() == []
  end

  test "writing no entries is a no-op" do
    assert :ok = CachexStore.put_entries(@target, [])
    assert CachexStore.size(@target) == 0
  end
end
