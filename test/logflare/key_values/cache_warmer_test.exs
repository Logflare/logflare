defmodule Logflare.KeyValues.CacheWarmerTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.CacheWarmer

  @pt_key {CacheWarmer, :initialized}
  @status_key {PeerWarmer, Cache}

  setup do
    :persistent_term.erase(@pt_key)
    :persistent_term.erase(@status_key)
    on_exit(fn -> :persistent_term.erase(@status_key) end)
    user = insert(:user)
    [user: user]
  end

  describe "initial warm (top-N by recency)" do
    test "populates cache with all key_values", %{user: user} do
      kv1 = insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
      kv2 = insert(:key_value, user: user, key: "k2", value: %{"v" => "2"})

      CacheWarmer.execute(nil)

      assert {:cached, kv1.value} == Cachex.get!(Cache, {:lookup, [user.id, "k1", nil]})
      assert {:cached, kv2.value} == Cachex.get!(Cache, {:lookup, [user.id, "k2", nil]})
    end

    test "warms only the top-N rows ordered by recency of use", %{user: user} do
      kv_used = insert(:key_value, user: user, key: "used", value: %{"v" => "used"})
      insert(:key_value, user: user, key: "unused", value: %{"v" => "unused"})

      insert(:key_value_usage, key_value: kv_used, last_used_at: DateTime.utc_now())

      prev = Application.get_env(:logflare, CacheWarmer, [])
      Application.put_env(:logflare, CacheWarmer, Keyword.put(prev, :warm_limit, 1))
      on_exit(fn -> Application.put_env(:logflare, CacheWarmer, prev) end)

      CacheWarmer.execute(nil)

      assert {:cached, kv_used.value} == Cachex.get!(Cache, {:lookup, [user.id, "used", nil]})
      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "unused", nil]}))
    end

    test "falls back to updated_at recency when no usage rows exist", %{user: user} do
      old_time = DateTime.add(DateTime.utc_now(), -2, :hour)

      Repo.insert!(%Logflare.KeyValues.KeyValue{
        user_id: user.id,
        key: "old",
        value: %{"v" => "old"},
        inserted_at: old_time,
        updated_at: old_time
      })

      kv_new = insert(:key_value, user: user, key: "new", value: %{"v" => "new"})

      prev = Application.get_env(:logflare, CacheWarmer, [])
      Application.put_env(:logflare, CacheWarmer, Keyword.put(prev, :warm_limit, 1))
      on_exit(fn -> Application.put_env(:logflare, CacheWarmer, prev) end)

      CacheWarmer.execute(nil)

      assert {:cached, kv_new.value} == Cachex.get!(Cache, {:lookup, [user.id, "new", nil]})
      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "old", nil]}))
    end

    test "marks itself as initialized after first run" do
      refute :persistent_term.get(@pt_key, false)

      CacheWarmer.execute(nil)

      assert :persistent_term.get(@pt_key, false)
    end

    test "if cache warmer fails, does not mark itself as initialized" do
      stub(Logflare.KeyValues.CacheWarmer, :warm_top_n, fn ->
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
      assert %{state: :warming} = PeerWarmer.status(Cache)
    end

    test "marks the cache ready with the time the database warm started" do
      started_at = DateTime.utc_now()

      CacheWarmer.execute(nil)

      assert %{state: :ready, warmed_at: warmed_at} = PeerWarmer.status(Cache)
      assert DateTime.compare(warmed_at, started_at) != :lt
    end

    test "returns :ignore", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})

      assert :ignore = CacheWarmer.execute(nil)
    end
  end

  describe "subsequent warm (recent records only)" do
    test "caches only recently inserted records", %{user: user} do
      old_time = DateTime.add(DateTime.utc_now(), -2, :hour)

      Repo.insert!(%Logflare.KeyValues.KeyValue{
        user_id: user.id,
        key: "old_key",
        value: %{"v" => "old"},
        inserted_at: old_time,
        updated_at: old_time
      })

      insert(:key_value, user: user, key: "new_key", value: %{"v" => "new"})

      # Mark as initialized to simulate subsequent warm
      :persistent_term.put(@pt_key, true)

      CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "new"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "new_key", nil]})

      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "old_key", nil]}))
    end

    test "no-ops when no recent records exist" do
      :persistent_term.put(@pt_key, true)

      assert :ignore = CacheWarmer.execute(nil)
    end

    test "moves warmed_at forward" do
      :persistent_term.put(@pt_key, true)
      PeerWarmer.mark_ready(Cache, DateTime.add(DateTime.utc_now(), -1, :hour))
      started_at = DateTime.utc_now()

      CacheWarmer.execute(nil)

      assert %{state: :ready, warmed_at: warmed_at} = PeerWarmer.status(Cache)
      assert DateTime.compare(warmed_at, started_at) != :lt
    end

    test "warm_recent/0 loads rows updated in the last hour", %{user: user} do
      insert(:key_value, user: user, key: "new_key", value: %{"v" => "new"})

      CacheWarmer.warm_recent()

      assert {:cached, %{"v" => "new"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "new_key", nil]})
    end
  end

  describe "initial warm copied from a peer" do
    setup do
      Cachex.clear!(Cache)
      :ok
    end

    test "skips the top-N database warm and catches up on rows updated since the peer's warm",
         %{user: user} do
      insert_key_value_updated_at(
        user,
        "before_peer_warm",
        DateTime.add(DateTime.utc_now(), -2, :hour)
      )

      insert(:key_value, user: user, key: "after_peer_warm", value: %{"v" => "new"})
      peer_warmed_at = DateTime.add(DateTime.utc_now(), -30, :minute)

      stub(PeerWarmer, :copy_from_peer, fn Cache ->
        {:ok, %{node: :peer@host, warmed_at: peer_warmed_at, count: 10}}
      end)

      reject(CacheWarmer, :warm_top_n, 0)
      started_at = DateTime.utc_now()

      assert :ignore = CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "new"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "after_peer_warm", nil]})

      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "before_peer_warm", nil]}))
      assert :persistent_term.get(@pt_key, false)
      assert %{state: :ready, warmed_at: warmed_at} = PeerWarmer.status(Cache)
      assert DateTime.compare(warmed_at, started_at) != :lt
    end

    test "records when the warm completed, so copied entries don't count as usage" do
      stub(PeerWarmer, :copy_from_peer, fn Cache ->
        {:ok, %{node: :peer@host, warmed_at: DateTime.utc_now(), count: 10}}
      end)

      :persistent_term.erase({CacheWarmer, :warmed_at})
      started_at = DateTime.utc_now()

      CacheWarmer.execute(nil)

      assert %DateTime{} = completed_at = CacheWarmer.warmed_at()
      assert DateTime.compare(completed_at, started_at) != :lt
    end

    test "catches up with a margin before the peer's warmed_at", %{user: user} do
      peer_warmed_at = DateTime.utc_now()
      insert_key_value_updated_at(user, "in_margin", DateTime.add(peer_warmed_at, -30, :second))

      stub(PeerWarmer, :copy_from_peer, fn Cache ->
        {:ok, %{node: :peer@host, warmed_at: peer_warmed_at, count: 10}}
      end)

      CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "in_margin"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "in_margin", nil]})
    end

    test "falls back to the top-N database warm when no peer can be copied", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})
      stub(PeerWarmer, :copy_from_peer, fn Cache -> :fallback end)

      CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "1"}} == Cachex.get!(Cache, {:lookup, [user.id, "k1", nil]})
      assert %{state: :ready} = PeerWarmer.status(Cache)
    end
  end

  defp insert_key_value_updated_at(user, key, updated_at) do
    Repo.insert!(%Logflare.KeyValues.KeyValue{
      user_id: user.id,
      key: key,
      value: %{"v" => key},
      inserted_at: updated_at,
      updated_at: updated_at
    })
  end
end
