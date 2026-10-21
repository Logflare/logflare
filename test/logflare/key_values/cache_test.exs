defmodule Logflare.KeyValues.CacheTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache
  alias Logflare.ContextCache.Gossip
  alias Logflare.ContextCache.Tombstones
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

  test "lookup/3 on a warmed entry", %{user: user} do
    value = %{"org" => %{"id" => "abc", "name" => "Acme"}}
    insert(:key_value, user: user, key: "proj1", value: value)
    KeyValues.CacheWarmer.warm_recent()
    reject(&KeyValues.lookup/2)
    reject(&KeyValues.lookup/3)

    for {accessor_path, expected} <- [
          {"org.id", "abc"},
          {"$.org.name", "Acme"},
          {"org.missing", nil}
        ] do
      assert KeyValues.Cache.lookup(user.id, "proj1", accessor_path) == expected
    end
  end

  test "lookup/2 returns nil for missing keys", %{user: user} do
    assert nil == KeyValues.Cache.lookup(user.id, "nonexistent")
  end

  test "busting a cached lookup and its accessor variants", %{user: user} do
    kv = insert(:key_value, user: user, key: "proj1", value: %{"org" => %{"id" => "abc"}})

    assert "abc" = KeyValues.Cache.lookup(user.id, "proj1", "org.id")
    assert %{"org" => %{"id" => "abc"}} = KeyValues.Cache.lookup(user.id, "proj1")

    new_value = %{"org" => %{"id" => "xyz"}}
    kv |> Ecto.Changeset.change(value: new_value) |> Repo.update!()
    assert {:ok, _} = ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "proj1"}])

    assert "xyz" = KeyValues.Cache.lookup(user.id, "proj1", "org.id")
    assert ^new_value = KeyValues.Cache.lookup(user.id, "proj1")
  end

  test "busting a key that is not cached", %{user: user} do
    assert {:ok, 0} = ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "nonexistent"}])
  end

  describe "count/1" do
    test "caches the count for a user", %{user: user} do
      insert(:key_value, user: user, key: "k1")
      insert(:key_value, user: user, key: "k2")

      assert KeyValues.Cache.count(user.id) == 2
      # Second call hits cache
      assert KeyValues.Cache.count(user.id) == 2
    end

    test "busting a cached count", %{user: user} do
      insert(:key_value, user: user, key: "k1")

      # populate cache
      assert 1 = KeyValues.Cache.count(user.id)

      # bust clears it
      ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "k1"}])

      cache_key = {:count, user.id}
      assert is_nil(Cachex.get!(KeyValues.Cache, cache_key))
    end
  end

  describe "tombstones/1" do
    for {name, kw, expected} <- [
          {"user and key", [user_id: 1, key: "k"], [{:count, 1}, {:key, 1, "k"}]},
          {"user only", [user_id: 1], [{:count, 1}]},
          {"no user", [key: "k"], []}
        ] do
      test name do
        assert KeyValues.Cache.tombstones(unquote(kw)) == unquote(Macro.escape(expected))
      end
    end

    test "primary key" do
      assert KeyValues.Cache.tombstones(1) == []
    end
  end

  describe "stale_entry?/2" do
    setup do
      Cachex.clear!(Tombstones.Cache)
      Gossip.record_tombstones([{KeyValues, [user_id: 1, key: "changed"]}])
      :ok
    end

    test "lookups, for every accessor path, of a changed key" do
      assert KeyValues.Cache.stale_entry?({:lookup, [1, "changed", nil]}, %{})
      assert KeyValues.Cache.stale_entry?({:lookup, [1, "changed", "org.id"]}, "abc")
      refute KeyValues.Cache.stale_entry?({:lookup, [1, "unchanged", nil]}, %{})
      refute KeyValues.Cache.stale_entry?({:lookup, [2, "changed", nil]}, %{})
    end

    test "counts of a user with a changed key" do
      assert KeyValues.Cache.stale_entry?({:count, 1}, 3)
      refute KeyValues.Cache.stale_entry?({:count, 2}, 3)
    end

    test "unknown key" do
      assert KeyValues.Cache.stale_entry?(:unknown, %{})
    end
  end
end
