defmodule Logflare.Cache.CachexOpsTest do
  use ExUnit.Case, async: false

  import Cachex.Spec

  alias Logflare.Cache.CachexOps

  @cache __MODULE__.Cache

  describe "child_spec/2" do
    for {stats_enabled?, expected} <- [{true, :ok}, {false, :error}] do
      test "cache stats config set to #{stats_enabled?}" do
        original = Application.get_env(:logflare, :cache_stats)
        on_exit(fn -> Application.put_env(:logflare, :cache_stats, original) end)
        Application.put_env(:logflare, :cache_stats, unquote(stats_enabled?))

        start_cache(limit: 10)
        Cachex.put(@cache, :key, :value)

        assert {unquote(expected), _} = Cachex.stats(@cache)
        assert :ok = CachexOps.reset(@cache)
        assert {:ok, 0} = Cachex.size(@cache)
      end
    end
  end

  describe "cachex_opts/1" do
    for {name, opts, expected} <- [
          {"only a nil limit", [limit: nil],
           [
             hooks: [],
             expiration: expiration(default: :timer.minutes(20), interval: :timer.minutes(5)),
             warmers: [],
             compressed: false
           ]},
          {"an integer limit", [limit: 10],
           [hooks: [hook(module: Cachex.Limit.Scheduled, args: {10, [], []})]]},
          {"a warmer module", [limit: nil, warmer: __MODULE__.Warmer],
           [
             warmers: [
               warmer(module: __MODULE__.Warmer, name: __MODULE__.Warmer, required: false)
             ]
           ]},
          {"a warmer with interval", [limit: nil, warmer: {__MODULE__.Warmer, interval: 1_000}],
           [
             warmers: [
               warmer(
                 module: __MODULE__.Warmer,
                 name: __MODULE__.Warmer,
                 required: false,
                 interval: 1_000
               )
             ]
           ]},
          {"ttl and purge interval", [limit: nil, ttl: 10_000, purge_interval: 2_000],
           [expiration: expiration(default: 10_000, interval: 2_000, lazy: true)]},
          {"compression", [limit: nil, compressed: true], [compressed: true]}
        ] do
      test name do
        opts = CachexOps.cachex_opts(unquote(Macro.escape(opts)))

        for {key, value} <- unquote(Macro.escape(expected)) do
          assert Keyword.fetch!(opts, key) == value
        end
      end
    end

    test "missing limit" do
      assert_raise KeyError, fn -> CachexOps.cachex_opts(compressed: true) end
    end
  end

  describe "healthy?/1" do
    test "started cache" do
      start_cache(limit: 10)
      assert CachexOps.healthy?(@cache)
    end

    test "cache that is not started" do
      refute CachexOps.healthy?(@cache)
    end
  end

  describe "stats/1" do
    test "cache after reads and writes" do
      start_cache(limit: 10)
      Cachex.put(@cache, :key, :value)
      Cachex.get(@cache, :key)
      Cachex.get(@cache, :missing)

      assert %{hits: 1, misses: 1, total_heap_size: heap} = stats = CachexOps.stats(@cache)
      assert heap > 0

      assert stats |> Map.keys() |> Enum.sort() == [
               :evictions,
               :expirations,
               :hit_rate,
               :hits,
               :miss_rate,
               :misses,
               :operations,
               :total_heap_size
             ]
    end
  end

  describe "reset/1" do
    test "cache with entries and stats" do
      start_cache(limit: 10)
      Cachex.put(@cache, :key, :value)
      Cachex.get(@cache, :key)

      assert :ok = CachexOps.reset(@cache)

      assert {:ok, nil} = Cachex.get(@cache, :key)
      assert %{hits: 0} = CachexOps.stats(@cache)
    end
  end

  describe "delete_keys/2" do
    setup do
      start_cache(limit: nil)
      Cachex.put_many(@cache, a: 1, b: 2)
      :ok
    end

    for {name, keys, expected_count} <- [
          {"existing keys", [:a, :b], 2},
          {"missing keys", [:x, :y], 0},
          {"existing and missing keys", [:a, :x], 1},
          {"no keys", [], 0}
        ] do
      test name do
        keys = unquote(keys)

        assert {:ok, unquote(expected_count)} = CachexOps.delete_keys(@cache, keys)

        for key <- keys do
          assert {:ok, false} = Cachex.exists?(@cache, key)
        end
      end
    end
  end

  describe "keys_to_bust/2" do
    setup do
      start_cache(limit: nil)
      :ok
    end

    for {name, value, expected_keys} <- [
          {"map with the id", %{id: 1}, [:key]},
          {":ok tuple with a map with the id", {:ok, %{id: 1}}, [:key]},
          {":ok 3-tuple with a map with the id", {:ok, %{id: 1}, :extra}, [:key]},
          {"list containing a map with the id", [%{id: 2}, %{id: 1}], [:key]},
          {"map with another id", %{id: 2}, []},
          {"list without the id", [%{id: 2}], []},
          {"nil", nil, []}
        ] do
      test "cached #{name}" do
        Cachex.put(@cache, :key, {:cached, unquote(Macro.escape(value))})

        assert Enum.to_list(CachexOps.keys_to_bust(@cache, id: 1)) == unquote(expected_keys)
      end
    end

    test "keyword other than id" do
      assert_raise ArgumentError, fn -> CachexOps.keys_to_bust(@cache, source_id: 1) end
    end
  end

  describe "fetch/3" do
    setup do
      start_cache(limit: nil)
      :ok
    end

    for value <- [:value, nil] do
      test "missing key with a getter returning #{inspect(value)}" do
        value = unquote(value)

        assert CachexOps.fetch(@cache, :key, fn -> value end) == value
        assert CachexOps.fetch(@cache, :key, fn -> flunk("getter called on a hit") end) == value
      end
    end
  end

  describe "update/3" do
    setup do
      start_cache(limit: nil)
      :ok
    end

    test "cached key" do
      CachexOps.fetch(@cache, :key, fn -> :old end)

      assert :ok = CachexOps.update(@cache, :key, :new)
      assert :new = CachexOps.fetch(@cache, :key, fn -> flunk("getter called on a hit") end)
    end

    test "missing key" do
      assert :ok = CachexOps.update(@cache, :key, :new)
      assert {:ok, false} = Cachex.exists?(@cache, :key)
    end
  end

  defp start_cache(opts), do: start_supervised!(CachexOps.child_spec(@cache, opts))
end
