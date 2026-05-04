defmodule Logflare.KeyValues.CacheTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  alias Logflare.KeyValues

  setup do
    user = insert(:user)
    [user: user]
  end

  test "lookup/2 caches results", %{user: user} do
    value = %{"org_id" => "org_abc"}
    insert(:key_value, user: user, key: "proj1", value: value)

    # first call populates cache
    assert ^value = KeyValues.Cache.lookup(user.id, "proj1")
    cache_key = {:lookup, [user.id, "proj1", nil]}
    assert {:cached, ^value} = Cachex.get!(KeyValues.Cache, cache_key)
  end

  test "lookup/3 caches extracted value with accessor path", %{user: user} do
    value = %{"org" => %{"id" => "abc", "name" => "Acme"}}
    insert(:key_value, user: user, key: "proj1", value: value)

    assert "abc" = KeyValues.Cache.lookup(user.id, "proj1", "org.id")
    cache_key = {:lookup, [user.id, "proj1", "org.id"]}
    assert {:cached, "abc"} = Cachex.get!(KeyValues.Cache, cache_key)
  end

  test "lookup/2 returns nil for missing keys", %{user: user} do
    assert nil == KeyValues.Cache.lookup(user.id, "nonexistent")
  end

  test "bust_by/1 clears cached lookup and all accessor variants", %{user: user} do
    value = %{"org" => %{"id" => "abc"}}
    insert(:key_value, user: user, key: "proj1", value: value)

    # populate cache with different accessor paths
    assert ^value = KeyValues.Cache.lookup(user.id, "proj1")
    assert "abc" = KeyValues.Cache.lookup(user.id, "proj1", "org.id")

    # bust clears both lookups + count entry (if cached)
    assert {:ok, busted} = KeyValues.Cache.bust_by(user_id: user.id, key: "proj1")
    assert busted >= 2
    assert is_nil(Cachex.get!(KeyValues.Cache, {:lookup, [user.id, "proj1", nil]}))
    assert is_nil(Cachex.get!(KeyValues.Cache, {:lookup, [user.id, "proj1", "org.id"]}))
  end

  test "bust_by/1 returns 0 when key not cached", %{user: user} do
    assert {:ok, 0} = KeyValues.Cache.bust_by(user_id: user.id, key: "nonexistent")
  end

  describe "count/1" do
    test "caches the count for a user", %{user: user} do
      insert(:key_value, user: user, key: "k1")
      insert(:key_value, user: user, key: "k2")

      assert KeyValues.Cache.count(user.id) == 2
      # Second call hits cache
      assert KeyValues.Cache.count(user.id) == 2
    end

    test "bust_by/1 clears cached count", %{user: user} do
      insert(:key_value, user: user, key: "k1")

      # populate cache
      assert 1 = KeyValues.Cache.count(user.id)

      # bust clears it
      KeyValues.Cache.bust_by(user_id: user.id, key: "k1")

      cache_key = {:count, user.id}
      assert is_nil(Cachex.get!(KeyValues.Cache, cache_key))
    end
  end
end
