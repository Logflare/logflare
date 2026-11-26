defmodule Logflare.KeyValues.CacheTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache
  alias Logflare.KeyValues
  alias Logflare.KeyValues.CacheWarmer
  alias Logflare.KeyValues.KeyFilter

  setup do
    :persistent_term.erase({CacheWarmer, :warmed_at})
    KeyFilter.rebuild(0, fn _filter -> :ok end)
    on_exit(&KeyFilter.clear/0)
    user = insert(:user)
    [user: user]
  end

  test "lookup/2 fetches and caches a key in the filter", %{user: user} do
    value = %{"org_id" => "org_abc"}
    insert(:key_value, user: user, key: "proj1", value: value)
    KeyFilter.add(user.id, "proj1")

    assert ^value = KeyValues.Cache.lookup(user.id, "proj1")
    cache_key = {:lookup, [user.id, "proj1", nil]}
    assert {:cached, ^value} = Cachex.get!(KeyValues.Cache, cache_key)
  end

  test "lookup/3 fetches and caches an extracted value for a key in the filter", %{user: user} do
    value = %{"org" => %{"id" => "abc", "name" => "Acme"}}
    insert(:key_value, user: user, key: "proj1", value: value)
    KeyFilter.add(user.id, "proj1")

    assert "abc" = KeyValues.Cache.lookup(user.id, "proj1", "org.id")
    cache_key = {:lookup, [user.id, "proj1", "org.id"]}
    assert {:cached, "abc"} = Cachex.get!(KeyValues.Cache, cache_key)
  end

  test "a key missing from the filter returns nil without a query or a cache entry",
       %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: %{"org" => "a"})
    reject(&KeyValues.lookup/2)
    reject(&KeyValues.lookup/3)

    assert KeyValues.Cache.lookup(user.id, "proj1") == nil
    assert KeyValues.Cache.lookup(user.id, "proj1", "org") == nil
    assert Cachex.get!(KeyValues.Cache, {:lookup, [user.id, "proj1", nil]}) == nil
    assert Cachex.get!(KeyValues.Cache, {:lookup, [user.id, "proj1", "org"]}) == nil
  end

  test "every key reads as absent before the filter is built", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: %{"org" => "a"})
    KeyFilter.clear()
    reject(&KeyValues.lookup/2)

    assert KeyValues.Cache.lookup(user.id, "proj1") == nil
  end

  test "a key in the filter without a row caches nil", %{user: user} do
    KeyFilter.add(user.id, "gone")

    assert KeyValues.Cache.lookup(user.id, "gone") == nil
    assert {:cached, nil} = Cachex.get!(KeyValues.Cache, {:lookup, [user.id, "gone", nil]})
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

  test "a WAL insert adds the key to the filter", %{user: user} do
    insert(:key_value, user: user, key: "new", value: %{"org" => "a"})
    assert KeyValues.Cache.lookup(user.id, "new") == nil

    assert {:ok, _} =
             ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "new", exists: true}])

    assert KeyFilter.maybe_exists?(user.id, "new")
    assert KeyValues.Cache.lookup(user.id, "new") == %{"org" => "a"}
  end

  test "a WAL delete does not add the key to the filter", %{user: user} do
    assert {:ok, _} = ContextCache.bust_keys([{KeyValues, user_id: user.id, key: "gone"}])

    refute KeyFilter.maybe_exists?(user.id, "gone")
  end

  test "busting a cached lookup and its accessor variants", %{user: user} do
    kv = insert(:key_value, user: user, key: "proj1", value: %{"org" => %{"id" => "abc"}})
    KeyFilter.add(user.id, "proj1")

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

  describe "WAL changes" do
    setup %{user: user} do
      kv = insert(:key_value, user: user, key: "proj", value: %{"org" => "a"})
      {:ok, _} = CacheWarmer.warm_all()
      assert "a" = KeyValues.Cache.lookup(user.id, "proj", "org")
      [kv: kv]
    end

    test "an insert makes the key visible", %{user: user} do
      insert(:key_value, user: user, key: "new", value: %{"org" => "b"})
      assert KeyValues.Cache.lookup(user.id, "new") == nil

      wal_bust(user.id, "new", exists: true)

      assert KeyValues.Cache.lookup(user.id, "new", "org") == "b"
    end

    test "an update replaces the cached value", %{user: user, kv: kv} do
      update_value(kv, %{"org" => "b"})

      wal_bust(user.id, "proj", exists: true)

      assert KeyValues.Cache.lookup(user.id, "proj", "org") == "b"
      assert KeyValues.Cache.lookup(user.id, "proj") == %{"org" => "b"}
    end

    test "a delete makes the key read as absent", %{user: user, kv: kv} do
      Repo.delete!(kv)

      wal_bust(user.id, "proj", [])

      assert KeyValues.Cache.lookup(user.id, "proj", "org") == nil
      assert KeyValues.Cache.lookup(user.id, "proj") == nil
    end

    test "a missed insert reads as absent until the hourly warm", %{user: user} do
      insert(:key_value, user: user, key: "new", value: %{"org" => "b"})
      assert KeyValues.Cache.lookup(user.id, "new") == nil

      hourly_warm()

      assert KeyValues.Cache.lookup(user.id, "new", "org") == "b"
    end

    test "a missed insert is visible after a full re-warm", %{user: user} do
      insert(:key_value, user: user, key: "new", value: %{"org" => "b"})

      full_rewarm()

      assert KeyFilter.maybe_exists?(user.id, "new")
      assert KeyValues.Cache.lookup(user.id, "new", "org") == "b"
    end

    test "a missed update serves the old value until the hourly warm", %{user: user, kv: kv} do
      update_value(kv, %{"org" => "b"})
      assert KeyValues.Cache.lookup(user.id, "proj") == %{"org" => "a"}

      hourly_warm()

      assert KeyValues.Cache.lookup(user.id, "proj") == %{"org" => "b"}
    end

    test "a missed delete serves the old value until a full re-warm", %{user: user, kv: kv} do
      Repo.delete!(kv)
      hourly_warm()
      assert KeyValues.Cache.lookup(user.id, "proj") == %{"org" => "a"}
      assert KeyValues.Cache.lookup(user.id, "proj", "org") == "a"

      full_rewarm()
      reject(&KeyValues.lookup/2)
      reject(&KeyValues.lookup/3)

      refute KeyFilter.maybe_exists?(user.id, "proj")
      assert KeyValues.Cache.lookup(user.id, "proj") == nil
      assert KeyValues.Cache.lookup(user.id, "proj", "org") == nil
    end

    test "a missed delete is dropped once its value expires", %{user: user, kv: kv} do
      Repo.delete!(kv)

      Cachex.expire!(KeyValues.Cache, {:lookup, [user.id, "proj", nil]}, 1)
      Cachex.expire!(KeyValues.Cache, {:lookup, [user.id, "proj", "org"]}, 1)
      Process.sleep(5)

      assert KeyValues.Cache.lookup(user.id, "proj") == nil
      assert KeyValues.Cache.lookup(user.id, "proj", "org") == nil
    end
  end

  describe "touch_recent_usages/1" do
    alias Logflare.KeyValues.KeyValueUsage

    setup %{user: user} do
      kv1 = insert(:key_value, user: user, key: "k1")
      kv2 = insert(:key_value, user: user, key: "k2")
      [kv1: kv1, kv2: kv2]
    end

    test "duplicated accessor-path", %{user: user, kv1: %{id: kv1_id}} do
      for path <- [nil, "org.id", "$.org.id"] do
        {:ok, true} =
          Cachex.put(KeyValues.Cache, {:lookup, [user.id, "k1", path]}, {:cached, "v"})
      end

      assert {:ok, 1} = KeyValues.Cache.touch_recent_usages(DateTime.utc_now())
      assert [%KeyValueUsage{key_value_id: ^kv1_id}] = Repo.all(KeyValueUsage)
    end

    test "count key", %{user: user} do
      {:ok, true} = Cachex.put(KeyValues.Cache, {:count, user.id}, {:cached, 5})

      assert {:ok, 0} = KeyValues.Cache.touch_recent_usages(DateTime.utc_now())
      assert [] = Repo.all(KeyValueUsage)
    end

    test "outside window", %{user: user} do
      {:ok, true} = Cachex.put(KeyValues.Cache, {:lookup, [user.id, "k1", nil]}, {:cached, "v"})

      # Passing a future `now` makes cutoff = future - 20min, which is after the
      # entry's modified time (≈ real now), so the entry falls outside the window.
      future = DateTime.add(DateTime.utc_now(), 25, :minute)
      assert {:ok, 0} = KeyValues.Cache.touch_recent_usages(future)
      assert [] = Repo.all(KeyValueUsage)
    end

    test "entries written by the initial warm", %{user: user, kv1: %{id: kv1_id, key: key}} do
      CacheWarmer.execute(nil)
      assert {:ok, 0} = KeyValues.Cache.touch_recent_usages(DateTime.utc_now())

      Process.sleep(2)
      {:ok, true} = Cachex.put(KeyValues.Cache, {:lookup, [user.id, key, nil]}, {:cached, "v"})

      assert {:ok, 1} = KeyValues.Cache.touch_recent_usages(DateTime.utc_now())
      assert [%KeyValueUsage{key_value_id: ^kv1_id}] = Repo.all(KeyValueUsage)
    end

    test "multiple users", %{user: user, kv1: kv1, kv2: kv2} do
      other = insert(:user)
      kv_other = insert(:key_value, user: other, key: "k3")

      for {uid, key} <- [{user.id, "k1"}, {user.id, "k2"}, {other.id, "k3"}] do
        {:ok, true} = Cachex.put(KeyValues.Cache, {:lookup, [uid, key, nil]}, {:cached, "v"})
      end

      assert {:ok, 3} = KeyValues.Cache.touch_recent_usages(DateTime.utc_now())

      ids = Repo.all(KeyValueUsage) |> MapSet.new(& &1.key_value_id)
      assert ids == MapSet.new([kv1.id, kv2.id, kv_other.id])
    end
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

  defp wal_bust(user_id, key, opts) do
    assert {:ok, _} = ContextCache.bust_keys([{KeyValues, [user_id: user_id, key: key] ++ opts}])
  end

  defp update_value(kv, value) do
    kv |> Ecto.Changeset.change(value: value) |> Repo.update!()
  end

  defp hourly_warm do
    :persistent_term.put({CacheWarmer, :warmed_at}, DateTime.utc_now())
    CacheWarmer.execute(nil)
  end

  defp full_rewarm do
    Process.sleep(2)
    :persistent_term.put({CacheWarmer, :warmed_at}, DateTime.add(DateTime.utc_now(), -13, :hour))
    CacheWarmer.execute(nil)
    assert DateTime.diff(DateTime.utc_now(), CacheWarmer.warmed_at()) < 60
  end
end
