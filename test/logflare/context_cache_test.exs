defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  import Logflare.Factory

  alias Logflare.ContextCache
  alias Logflare.Sources

  setup do
    user = insert(:user)
    insert(:plan, name: "Free")
    source = insert(:source, user: user)
    args = [token: source.token]
    source = Sources.Cache.get_by(args)
    fun = :get_by
    cache_key = {fun, [args]}
    %{source: source, cache_key: cache_key}
  end

  test "cache_name/1", %{source: source} do
    assert Sources.Cache == ContextCache.cache_name(Sources)
  end

  test "apply_fun/3", %{source: source, cache_key: cache_key} do
    assert {:cached, %Logflare.Source{}} = Cachex.get!(Sources.Cache, cache_key)
  end

  test "bust_keys/1", %{source: source, cache_key: cache_key} do
    assert {:ok, :busted} = ContextCache.bust_keys([{Sources, source.id}])
    assert is_nil(Cachex.get!(Sources.Cache, cache_key))
    match = {:entry, {{Sources, source.id}, :_}, :_, :_, :"$1"}
    assert [] = :ets.match(ContextCache, match)
  end
end
