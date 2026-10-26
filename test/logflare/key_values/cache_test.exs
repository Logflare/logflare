defmodule Logflare.KeyValues.CacheTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache
  alias Logflare.ContextCache.CacheBusterWorker
  alias Logflare.KeyValues
  alias Logflare.KeyValues.CacheWarmer

  @old_value %{"org" => %{"id" => "old"}}
  @new_value %{"org" => %{"id" => "new"}}

  setup do
    :persistent_term.erase({CacheWarmer, :initialized})
    :persistent_term.erase({CacheWarmer, :warmed_at})
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

  test "lookup filled while its key-value changes", %{user: user} do
    for {change, accessor_path, filled, expected} <- [
          {:update, nil, @old_value, @new_value},
          {:update, "org.id", "old", "new"},
          {:delete, nil, @old_value, nil},
          {:delete, "org.id", "old", nil},
          {:insert, nil, nil, @new_value},
          {:insert, "org.id", nil, "new"}
        ] do
      key = "#{change}:#{accessor_path}"
      if change != :insert, do: insert(:key_value, user: user, key: key, value: @old_value)
      change_mid_fill(user, key, key, change)

      assert KeyValues.Cache.lookup(user.id, key, accessor_path) == filled
      assert KeyValues.Cache.lookup(user.id, key, accessor_path) == expected
    end
  end

  test "lookup filled while another key-value of the user changes", %{user: user} do
    insert(:key_value, user: user, key: "proj1", value: @old_value)
    insert(:key_value, user: user, key: "proj2", value: @old_value)
    change_mid_fill(user, "proj1", "proj2", :update)

    assert @old_value = KeyValues.Cache.lookup(user.id, "proj1")
    reject(&KeyValues.lookup/3)
    assert @old_value = KeyValues.Cache.lookup(user.id, "proj1")
  end

  test "lookup of a key-value changed longer than the tombstone ttl ago", %{user: user} do
    original = Application.fetch_env!(:logflare, KeyValues.Cache)
    Application.put_env(:logflare, KeyValues.Cache, Keyword.put(original, :tombstone_ttl, 1))
    on_exit(fn -> Application.put_env(:logflare, KeyValues.Cache, original) end)

    insert(:key_value, user: user, key: "proj1", value: @old_value)
    apply_change(user, "proj1", :update)
    bust(user.id, "proj1")
    Process.sleep(10)

    assert @new_value = KeyValues.Cache.lookup(user.id, "proj1")
    reject(&KeyValues.lookup/3)
    assert @new_value = KeyValues.Cache.lookup(user.id, "proj1")
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

  defp change_mid_fill(%{id: user_id} = user, filled_key, changed_key, change) do
    stub(KeyValues, :lookup, &Mimic.call_original(KeyValues, :lookup, [&1, &2, &3]))

    expect(KeyValues, :lookup, fn ^user_id, ^filled_key, accessor_path ->
      filled = Mimic.call_original(KeyValues, :lookup, [user_id, filled_key, accessor_path])
      apply_change(user, changed_key, change)
      bust(user_id, changed_key)
      filled
    end)
  end

  defp bust(user_id, key) do
    {:noreply, _state} =
      CacheBusterWorker.handle_cast(
        {:to_bust, [{KeyValues, user_id: user_id, key: key}], []},
        %{}
      )
  end

  defp apply_change(user, key, :insert),
    do: insert(:key_value, user: user, key: key, value: @new_value)

  defp apply_change(user, key, :update) do
    {:ok, kv} = KeyValues.fetch_key_value_by(user_id: user.id, key: key)
    {:ok, _kv} = KeyValues.update_key_value(kv, %{value: @new_value})
  end

  defp apply_change(user, key, :delete) do
    {:ok, kv} = KeyValues.fetch_key_value_by(user_id: user.id, key: key)
    {:ok, _kv} = KeyValues.delete_key_value(kv)
  end
end
