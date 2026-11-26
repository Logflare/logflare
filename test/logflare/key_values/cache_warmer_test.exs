defmodule Logflare.KeyValues.CacheWarmerTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Logflare.KeyValues.Cache
  alias Logflare.KeyValues.CacheWarmer
  alias Logflare.KeyValues.KeyFilter
  alias Logflare.KeyValues.KeyValue

  @pt_key {CacheWarmer, :initialized}
  @pt_warmed_at {CacheWarmer, :warmed_at}

  setup do
    :persistent_term.erase(@pt_key)
    :persistent_term.erase(@pt_warmed_at)
    KeyFilter.clear()
    on_exit(&KeyFilter.clear/0)
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

    for {context, keys, limit, warmed} <- [
          {"usage rows beyond warm_limit", [{"a", used: 1}, {"b", used: 2}, {"unused", []}], 1,
           ["a"]},
          {"usage rows below warm_limit",
           [{"old_unused", []}, {"used", used: 5}, {"new_unused", []}], 2,
           ["used", "new_unused"]},
          {"empty usage table", [{"old", []}, {"new", []}], 1, ["new"]},
          {"unused key updated after a newer insert",
           [{"updated", updated: 1}, {"newer", updated: 5}], 1, ["updated"]}
        ] do
      test context, %{user: user} do
        insert_keys(user, unquote(keys))

        assert {:ok, :ok} = CacheWarmer.warm_top_n(unquote(limit))

        assert cached_keys(user, unquote(keys)) == unquote(warmed)
      end
    end

    test "marks itself as initialized after first run" do
      refute :persistent_term.get(@pt_key, false)

      CacheWarmer.execute(nil)

      assert :persistent_term.get(@pt_key, false)
    end

    test "if cache warmer fails, does not mark itself as initialized" do
      stub(Logflare.KeyValues.CacheWarmer, :warm_all, fn ->
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

  describe "filter and sweep" do
    test "the filter holds every key, including keys beyond the warm limit", %{user: user} do
      insert_keys(user, [{"a", used: 1}, {"b", []}])

      assert {:ok, 2} = CacheWarmer.rebuild_filter()
      assert {:ok, :ok} = CacheWarmer.warm_top_n(1)

      assert cached_keys(user, [{"a", []}, {"b", []}]) == ["a"]
      assert KeyFilter.maybe_exists?(user.id, "a")
      assert KeyFilter.maybe_exists?(user.id, "b")
      refute KeyFilter.maybe_exists?(user.id, "missing")
    end

    test "the filter rebuild pages through more keys than one page", %{user: user} do
      now = DateTime.utc_now()

      rows =
        for i <- 1..10_001 do
          %{user_id: user.id, key: "k#{i}", value: %{}, inserted_at: now, updated_at: now}
        end

      for chunk <- Enum.chunk_every(rows, 5_000), do: Repo.insert_all(KeyValue, chunk)

      assert {:ok, 10_001} = CacheWarmer.rebuild_filter()
      assert KeyFilter.maybe_exists?(user.id, "k1")
      assert KeyFilter.maybe_exists?(user.id, "k10001")
      assert %{size: 10_001} = KeyFilter.info()
    end

    test "sweep deletes cached lookups for keys missing from the filter only", %{user: user} do
      insert(:key_value, user: user, key: "kept")
      CacheWarmer.rebuild_filter()
      Cachex.put!(Cache, {:lookup, [user.id, "kept", nil]}, {:cached, %{}})
      Cachex.put!(Cache, {:lookup, [user.id, "kept", "a"]}, {:cached, 1})
      Cachex.put!(Cache, {:lookup, [user.id, "gone", nil]}, {:cached, %{}})
      Cachex.put!(Cache, {:lookup, [user.id, "gone", "a"]}, {:cached, 1})
      Cachex.put!(Cache, {:count, user.id}, {:cached, 1})

      assert CacheWarmer.sweep() >= 2

      assert {:cached, %{}} == Cachex.get!(Cache, {:lookup, [user.id, "kept", nil]})
      assert {:cached, 1} == Cachex.get!(Cache, {:lookup, [user.id, "kept", "a"]})
      assert Cachex.get!(Cache, {:lookup, [user.id, "gone", nil]}) == nil
      assert Cachex.get!(Cache, {:lookup, [user.id, "gone", "a"]}) == nil
      assert {:cached, 1} == Cachex.get!(Cache, {:count, user.id})
    end

    test "a full warm runs again once 12 hours have passed", %{user: user} do
      CacheWarmer.execute(nil)
      kv = insert(:key_value, user: user, key: "late", updated_at: minutes_ago(120))
      Cachex.put!(Cache, {:lookup, [user.id, "gone", nil]}, {:cached, %{}})

      CacheWarmer.execute(nil)
      assert Cachex.get!(Cache, {:lookup, [user.id, "late", nil]}) == nil

      :persistent_term.put(@pt_warmed_at, DateTime.add(DateTime.utc_now(), -13, :hour))
      CacheWarmer.execute(nil)

      assert {:cached, kv.value} == Cachex.get!(Cache, {:lookup, [user.id, "late", nil]})
      assert Cachex.get!(Cache, {:lookup, [user.id, "gone", nil]}) == nil
      assert KeyFilter.maybe_exists?(user.id, "late")
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
      :persistent_term.put(@pt_warmed_at, DateTime.utc_now())
      KeyFilter.rebuild(0, fn _filter -> :ok end)

      CacheWarmer.execute(nil)

      assert {:cached, %{"v" => "new"}} ==
               Cachex.get!(Cache, {:lookup, [user.id, "new_key", nil]})

      assert is_nil(Cachex.get!(Cache, {:lookup, [user.id, "old_key", nil]}))
      assert KeyFilter.maybe_exists?(user.id, "new_key")
      refute KeyFilter.maybe_exists?(user.id, "old_key")
    end

    test "no-ops when no recent records exist" do
      :persistent_term.put(@pt_key, true)
      :persistent_term.put(@pt_warmed_at, DateTime.utc_now())

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
