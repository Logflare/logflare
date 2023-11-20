defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  import Logflare.Factory

  alias Logflare.ContextCache
  alias Logflare.Sources

  setup do
    user = insert(:user)
    insert(:plan, name: "Free")
    source = insert(:source, user: user)
    %{source: source}
  end

  test "ContextCache works", %{source: source} do
    context = Sources
    fun = :get_by
    fun_arity = {fun, 1}
    args = [[token: source.token]]

    # Cache our function call results
    cached_source = ContextCache.apply_fun(context, fun_arity, args)

    cache_key = {fun, args}
    cache_name = ContextCache.cache_name(context)
    assert cache_name == Sources.Cache

    assert %Logflare.Source{} = cached_source

    # Make sure we have it in our context cache
    assert {:cached, %Logflare.Source{}} = Cachex.get!(cache_name, cache_key)

    # Should do this with the wal instead at some point
    assert {:ok, :busted} = ContextCache.bust_keys([{context, cached_source.id}])

    # Make sure we don't have it after it's busted
    assert is_nil(Cachex.get!(cache_name, cache_key))
  end
end
