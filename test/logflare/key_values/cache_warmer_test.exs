defmodule Logflare.KeyValues.CacheWarmerTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.CacheWarmer
  alias Logflare.KeyValues.KeyFilter

  @pt_warmed_at {CacheWarmer, :warmed_at}

  setup do
    :persistent_term.erase(@pt_warmed_at)
    KeyFilter.clear()
    on_exit(&KeyFilter.clear/0)
    user = insert(:user)
    [user: user]
  end

  describe "full warm" do
    test "loads every key value and builds the filter", %{user: user} do
      keys = [{"used", used: 1}, {"unused", []}, {"old", updated: 60 * 24 * 30}]
      insert_keys(user, keys)

      CacheWarmer.execute(nil)

      assert cached_keys(user, keys) == ["used", "unused", "old"]
      assert Enum.all?(keys, fn {key, _} -> KeyFilter.maybe_exists?(user.id, key) end)
      refute KeyFilter.maybe_exists?(user.id, "missing")
    end

    test "records when it completed" do
      assert CacheWarmer.warmed_at() == nil

      CacheWarmer.execute(nil)

      assert %DateTime{} = CacheWarmer.warmed_at()
    end

    test "drops cached keys that no longer exist", %{user: user} do
      kv = insert(:key_value, user: user, key: "kept", value: %{"v" => "1"})
      CacheWarmer.execute(nil)
      Cachex.put!(Cache, {:lookup, [user.id, "gone", nil]}, {:cached, %{"v" => "old"}})
      Cachex.put!(Cache, {:lookup, [user.id, "kept", "v"]}, {:cached, "1"})
      Cachex.put!(Cache, {:count, user.id}, {:cached, 2})
      Process.sleep(2)
      :persistent_term.put(@pt_warmed_at, DateTime.add(DateTime.utc_now(), -13, :hour))

      CacheWarmer.execute(nil)

      assert Cachex.get!(Cache, {:lookup, [user.id, "gone", nil]}) == nil
      assert Cachex.get!(Cache, {:lookup, [user.id, "kept", "v"]}) == nil
      assert {:cached, kv.value} == Cachex.get!(Cache, {:lookup, [user.id, "kept", nil]})
      assert {:cached, 2} == Cachex.get!(Cache, {:count, user.id})
    end

    test "if it fails, it logs and is retried on the next run" do
      stub(CacheWarmer, :warm_all, fn -> raise RuntimeError, "test" end)

      log = capture_log([level: :error], fn -> CacheWarmer.execute(nil) end)

      assert log =~ "Error performing full KeyValues.Cache warming"
      assert log =~ "RuntimeError"
      assert CacheWarmer.warmed_at() == nil
    end

    test "returns :ignore", %{user: user} do
      insert(:key_value, user: user, key: "k1", value: %{"v" => "1"})

      assert :ignore = CacheWarmer.execute(nil)
    end
  end

  describe "sweep/1" do
    test "deletes lookup entries written before the given time only", %{user: user} do
      Cachex.put!(Cache, {:lookup, [user.id, "old", nil]}, {:cached, 1})
      Cachex.put!(Cache, {:count, user.id}, {:cached, 1})
      Process.sleep(2)
      started_at = System.system_time(:millisecond)
      Process.sleep(2)
      Cachex.put!(Cache, {:lookup, [user.id, "new", nil]}, {:cached, 1})

      assert CacheWarmer.sweep(started_at) >= 1

      assert Cachex.get!(Cache, {:lookup, [user.id, "old", nil]}) == nil
      assert {:cached, 1} == Cachex.get!(Cache, {:lookup, [user.id, "new", nil]})
      assert {:cached, 1} == Cachex.get!(Cache, {:count, user.id})
    end
  end

  describe "subsequent warm (recent records only)" do
    setup do
      KeyFilter.rebuild(0, fn _filter -> :ok end)
      :persistent_term.put(@pt_warmed_at, DateTime.utc_now())
      :ok
    end

    test "caches only recently inserted records and adds them to the filter", %{user: user} do
      old_time = DateTime.add(DateTime.utc_now(), -2, :hour)

      Repo.insert!(%Logflare.KeyValues.KeyValue{
        user_id: user.id,
        key: "old_key",
        value: %{"v" => "old"},
        inserted_at: old_time,
        updated_at: old_time
      })

      insert(:key_value, user: user, key: "new_key", value: %{"v" => "new"})

      CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "new"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "new_key", nil]})

      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "old_key", nil]}))
      assert KeyFilter.maybe_exists?(user.id, "new_key")
      refute KeyFilter.maybe_exists?(user.id, "old_key")
    end

    test "no-ops when no recent records exist" do
      assert :ignore = CacheWarmer.execute(nil)
    end
  end

  defp insert_keys(user, keys) do
    for {key, opts} <- keys do
      updated_at = minutes_ago(Keyword.get(opts, :updated, 0))
      kv = insert(:key_value, user: user, key: key, updated_at: updated_at)

      if used = opts[:used] do
        insert(:key_value_usage, key_value: kv, last_used_at: minutes_ago(used))
      end
    end
  end

  defp minutes_ago(minutes), do: DateTime.add(DateTime.utc_now(), -minutes, :minute)

  defp cached_keys(user, keys) do
    for {key, _} <- keys, Cachex.get!(Cache, {:lookup, [user.id, key, nil]}), do: key
  end
end
