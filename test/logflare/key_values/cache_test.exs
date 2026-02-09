defmodule Logflare.KeyValues.CacheTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  alias Logflare.KeyValues

  setup do
    user = insert(:user)
    [user: user]
  end

  test "lookup/2 caches results", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: "org_abc")

    # first call populates cache
    assert "org_abc" = KeyValues.Cache.lookup(user.id, "proj1")
    cache_key = {:lookup, [user.id, "proj1"]}
    assert {:cached, "org_abc"} = Cachex.get!(KeyValues.Cache, cache_key)
  end

  test "lookup/2 returns nil for missing keys", %{user: user} do
    assert nil == KeyValues.Cache.lookup(user.id, "nonexistent")
  end

  test "bust_by/1 clears cached lookup", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: "org_abc")

    # populate cache
    assert "org_abc" = KeyValues.Cache.lookup(user.id, "proj1")

    # bust clears it
    assert {:ok, 1} = KeyValues.Cache.bust_by(user_id: user.id, key: "proj1")
    cache_key = {:lookup, [user.id, "proj1"]}
    assert is_nil(Cachex.get!(KeyValues.Cache, cache_key))
  end

  test "bust_by/1 returns 0 when key not cached", %{user: user} do
    assert {:ok, 0} = KeyValues.Cache.bust_by(user_id: user.id, key: "nonexistent")
  end

  describe "count/1" do
    test "caches the count for a user", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")
      insert(:key_value, user: user, key: "k2", value: "v2")

      assert KeyValues.Cache.count(user.id) == 2
      # Second call hits cache
      assert KeyValues.Cache.count(user.id) == 2
    end

    test "bust_by/1 clears cached count", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: "v1")

      # populate cache
      assert 1 = KeyValues.Cache.count(user.id)

      # bust clears it
      KeyValues.Cache.bust_by(user_id: user.id, key: "k1")

      cache_key = {:count, user.id}
      assert is_nil(Cachex.get!(KeyValues.Cache, cache_key))
    end
  end
end
