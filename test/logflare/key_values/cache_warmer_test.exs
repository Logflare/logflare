defmodule Logflare.KeyValues.CacheWarmerTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.CacheWarmer

  @pt_key {CacheWarmer, :initialized}

  setup do
    :persistent_term.erase(@pt_key)
    Cachex.clear!(Cache)
    user = insert(:user)
    [user: user]
  end

  describe "initial warm (full table stream)" do
    test "populates cache with all key_values", %{user: user} do
      kv1 = insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
      kv2 = insert(:key_value, user: user, key: "k2", value: %{"v" => "2"})

      CacheWarmer.execute(nil)

      assert {:cached, kv1.value} == Cachex.get!(Cache, {:lookup, [user.id, "k1", nil]})
      assert {:cached, kv2.value} == Cachex.get!(Cache, {:lookup, [user.id, "k2", nil]})
    end

    test "marks itself as initialized for the current cache process" do
      refute :persistent_term.get(@pt_key, false)

      CacheWarmer.execute(nil)

      assert :persistent_term.get(@pt_key) == Process.whereis(Cache)
    end

    test "performs a full warm when the marker belongs to an older cache process" do
      :persistent_term.put(@pt_key, self())

      expect(CacheWarmer, :warm_full, fn -> :ok end)
      reject(CacheWarmer, :warm_recent, 0)

      assert :ignore = CacheWarmer.execute(nil)
      assert :persistent_term.get(@pt_key) == Process.whereis(Cache)
    end

    test "if cache warmer fails, does not mark itself as initialized" do
      stub(Logflare.KeyValues.CacheWarmer, :warm_full, fn ->
        raise RuntimeError, "test"
      end)

      assert :persistent_term.get(@pt_key, false) == false

      log =
        capture_log([level: :error], fn ->
          CacheWarmer.execute(nil)
        end)

      assert log =~ "Error performing full KeyValues.Cache warming"
      assert log =~ "RuntimeError"
      assert log =~ "test"
      assert :persistent_term.get(@pt_key, false) == false
    end

    test "returns :ignore", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})

      assert :ignore = CacheWarmer.execute(nil)
    end
  end

  describe "subsequent warm (recent records only)" do
    test "caches only recently inserted records", %{user: user} do
      old_time = DateTime.add(DateTime.utc_now(), -3, :hour)

      Repo.insert!(%Logflare.KeyValues.KeyValue{
        user_id: user.id,
        key: "old_key",
        value: %{"v" => "old"},
        inserted_at: old_time,
        updated_at: old_time
      })

      insert(:key_value, user: user, key: "new_key", value: %{"v" => "new"})

      :persistent_term.put(@pt_key, Process.whereis(Cache))

      CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "new"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "new_key", nil]})

      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "old_key", nil]}))
    end

    test "no-ops when no recent records exist" do
      :persistent_term.put(@pt_key, Process.whereis(Cache))

      assert :ignore = CacheWarmer.execute(nil)
    end

    test "handles a transient recent-warm failure without crashing" do
      :persistent_term.put(@pt_key, Process.whereis(Cache))

      stub(CacheWarmer, :warm_recent, fn ->
        raise RuntimeError, "test"
      end)

      log =
        capture_log([level: :error], fn ->
          assert :ignore = CacheWarmer.execute(nil)
        end)

      assert log =~ "Error performing recent KeyValues.Cache warming"
      assert log =~ "RuntimeError"
      assert :persistent_term.get(@pt_key) == Process.whereis(Cache)
    end

    test "handles an exit from a recent warm without crashing" do
      :persistent_term.put(@pt_key, Process.whereis(Cache))

      stub(CacheWarmer, :warm_recent, fn -> exit(:database_unavailable) end)

      log =
        capture_log([level: :error], fn ->
          assert :ignore = CacheWarmer.execute(nil)
        end)

      assert log =~ "Error performing recent KeyValues.Cache warming"
      assert log =~ "database_unavailable"
    end

    test "drops a warm batch when an invalidation crosses its generation fence", %{user: user} do
      cache_key = {:lookup, [user.id, "stale", nil]}
      entries = [{cache_key, {:cached, %{"version" => "stale"}}}]
      generation = CacheWarmer.invalidation_generation()

      assert :ok = CacheWarmer.mark_invalidation()
      assert :invalidated = CacheWarmer.put_if_unchanged(entries, generation)
      assert {:ok, nil} = Cachex.get(Cache, cache_key)
    end

    test "propagates cache write failures" do
      generation = CacheWarmer.invalidation_generation()

      assert {:error, :invalid_pairs} = CacheWarmer.put_if_unchanged([:invalid], generation)
    end
  end
end
