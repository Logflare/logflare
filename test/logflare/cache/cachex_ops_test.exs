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

  describe "bust_by/2" do
    setup do
      start_cache(limit: nil)
      :ok
    end

    for {name, value, expected_count} <- [
          {"map with the id", %{id: 1}, 1},
          {":ok tuple with a map with the id", {:ok, %{id: 1}}, 1},
          {":ok 3-tuple with a map with the id", {:ok, %{id: 1}, :extra}, 1},
          {"list containing a map with the id", [%{id: 2}, %{id: 1}], 1},
          {"map with another id", %{id: 2}, 0},
          {"list without the id", [%{id: 2}], 0},
          {"nil", nil, 0}
        ] do
      test "cached #{name}" do
        Cachex.put(@cache, :key, {:cached, unquote(Macro.escape(value))})

        assert {:ok, unquote(expected_count)} = CachexOps.bust_by(@cache, id: 1)
        assert {:ok, unquote(expected_count == 0)} = Cachex.exists?(@cache, :key)
      end
    end

    test "keyword other than id" do
      assert_raise ArgumentError, fn -> CachexOps.bust_by(@cache, source_id: 1) end
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

  describe "entries/1" do
    setup do
      start_cache(limit: nil, ttl: to_timeout(minute: 10))
      :ok
    end

    test "unwrapped values with their remaining ttl" do
      CachexOps.fetch(@cache, :a, fn -> :value end)
      CachexOps.fetch(@cache, :b, fn -> nil end)

      assert [{:a, :value, ttl}, {:b, nil, _ttl}] = @cache |> CachexOps.entries() |> Enum.sort()
      assert ttl > to_timeout(minute: 9) and ttl <= to_timeout(minute: 10)
    end

    test "nil ttl for entries that do not expire" do
      cache = __MODULE__.NoExpiration
      start_supervised!(Supervisor.child_spec({Cachex, [cache, []]}, id: cache))
      Cachex.put(cache, :key, {:cached, :value})

      assert [{:key, :value, nil}] = cache |> CachexOps.entries() |> Enum.to_list()
    end

    test "skips expired entries and values not cached as {:cached, value}" do
      Cachex.put(@cache, :expired, {:cached, :a}, expire: 1)
      Cachex.put(@cache, :raw, :b)
      Process.sleep(5)

      assert @cache |> CachexOps.entries() |> Enum.to_list() == []
    end
  end

  describe "put_entries/2" do
    setup do
      start_cache(limit: nil, ttl: to_timeout(minute: 10))
      :ok
    end

    test "writes values readable by fetch/3, keeping their ttl" do
      assert :ok =
               CachexOps.put_entries(@cache, [
                 {:expiring, :a, to_timeout(minute: 2)},
                 {:not_expiring, nil, nil}
               ])

      assert :a = CachexOps.fetch(@cache, :expiring, fn -> flunk("getter called on a hit") end)

      assert nil ==
               CachexOps.fetch(@cache, :not_expiring, fn -> flunk("getter called on a hit") end)

      assert {:ok, ttl} = Cachex.ttl(@cache, :expiring)
      assert ttl > to_timeout(minute: 1) and ttl <= to_timeout(minute: 2)
      assert {:ok, default_ttl} = Cachex.ttl(@cache, :not_expiring)
      assert default_ttl > to_timeout(minute: 9)
    end

    test "replaces cached values" do
      CachexOps.fetch(@cache, :key, fn -> :old end)

      assert :ok = CachexOps.put_entries(@cache, [{:key, :new, nil}])
      assert :new = CachexOps.fetch(@cache, :key, fn -> flunk("getter called on a hit") end)
    end

    test "no entries" do
      assert :ok = CachexOps.put_entries(@cache, [])
      assert CachexOps.size(@cache) == 0
    end

    test "round trip through entries/1" do
      source = __MODULE__.Source
      start_supervised!(CachexOps.child_spec(source, limit: nil))
      CachexOps.fetch(source, :a, fn -> 1 end)
      CachexOps.fetch(source, :b, fn -> nil end)

      assert :ok = CachexOps.put_entries(@cache, Enum.to_list(CachexOps.entries(source)))

      assert [{:a, 1, _ttl_a}, {:b, nil, _ttl_b}] =
               @cache |> CachexOps.entries() |> Enum.sort()
    end
  end

  describe "expiry/2" do
    setup do
      start_cache(limit: nil, ttl: to_timeout(minute: 10))
      :ok
    end

    test "remaining and total ttl of an entry" do
      CachexOps.put_entries(@cache, [{:key, :value, to_timeout(minute: 2)}])

      assert {remaining, total} = CachexOps.expiry(@cache, :key)
      assert total == to_timeout(minute: 2)
      assert remaining > to_timeout(minute: 1) and remaining <= total
    end

    test "entries written by fetch/3 get the default ttl" do
      CachexOps.fetch(@cache, :key, fn -> :value end)

      assert {_remaining, total} = CachexOps.expiry(@cache, :key)
      assert total == to_timeout(minute: 10)
    end

    test "zero remaining for an expired entry that wasn't purged yet" do
      CachexOps.put_entries(@cache, [{:key, :value, 1}])
      Process.sleep(5)

      assert {0, 1} = CachexOps.expiry(@cache, :key)
    end

    test "nil when not cached" do
      assert nil == CachexOps.expiry(@cache, :missing)
    end
  end

  describe "cached?/2 and size/1" do
    setup do
      start_cache(limit: nil)
      :ok
    end

    test "empty cache" do
      refute CachexOps.cached?(@cache, :key)
      assert CachexOps.size(@cache) == 0
    end

    test "cached value, including nil" do
      CachexOps.fetch(@cache, :key, fn -> nil end)

      assert CachexOps.cached?(@cache, :key)
      refute CachexOps.cached?(@cache, :other)
      assert CachexOps.size(@cache) == 1
    end
  end

  defp start_cache(opts), do: start_supervised!(CachexOps.child_spec(@cache, opts))
end
