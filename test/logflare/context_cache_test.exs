defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
  alias Logflare.ContextCache.Tombstones
  alias Logflare.ContextCache.TransactionBroadcaster
  alias Logflare.Sources

  defmodule RecordingOps do
    @behaviour Logflare.Cache.Ops
    @behaviour Logflare.ContextCache.Ops

    @impl Logflare.Cache.Ops
    def healthy?(cache), do: record({:healthy?, cache}, true)
    @impl Logflare.Cache.Ops
    def stats(cache), do: record({:stats, cache}, %{})
    @impl Logflare.Cache.Ops
    def reset(cache), do: record({:reset, cache}, :ok)

    @impl Logflare.ContextCache.Ops
    def bust_by(cache, kw), do: record({:bust_by, cache, kw}, {:ok, 1})
    @impl Logflare.ContextCache.Ops
    def fetch(cache, key, _getter), do: record({:fetch, cache, key}, :fetched)
    @impl Logflare.ContextCache.Ops
    def update(cache, key, value), do: record({:update, cache, key, value}, :ok)
    @impl Logflare.ContextCache.Ops
    def entries(cache), do: record({:entries, cache}, [{:key, :value, nil}])
    @impl Logflare.ContextCache.Ops
    def put_entries(cache, entries), do: record({:put_entries, cache, entries}, :ok)
    @impl Logflare.ContextCache.Ops
    def cached?(cache, key), do: record({:cached?, cache, key}, true)
    @impl Logflare.ContextCache.Ops
    def entry(cache, key), do: record({:entry, cache, key}, {key, :value, 500})
    @impl Logflare.ContextCache.Ops
    def expiry(cache, key), do: record({:expiry, cache, key}, {500, 1_000})
    @impl Logflare.ContextCache.Ops
    def size(cache), do: record({:size, cache}, 1)

    defp record(call, result) do
      send(self(), call)
      result
    end
  end

  defmodule CustomImpl.Cache do
    use Logflare.ContextCache, impl: RecordingOps
  end

  test "bust_keys/1 with an empty list" do
    assert {:ok, 0} = ContextCache.bust_keys([])
  end

  describe "context cache with a custom impl module" do
    test "apply_fun/3" do
      assert :fetched = ContextCache.apply_fun(CustomImpl, :get, [1])
      assert_received {:fetch, CustomImpl.Cache, {:get, [1]}}
    end

    test "update/4" do
      assert :ok = ContextCache.update(CustomImpl, :get, [1], :value)
      assert_received {:update, CustomImpl.Cache, {:get, [1]}, :value}
    end

    test "entry callbacks" do
      assert [{:key, :value, nil}] = CustomImpl.Cache.entries()
      assert_received {:entries, CustomImpl.Cache}

      assert :ok = CustomImpl.Cache.put_entries([{:key, :value, 1_000}])
      assert_received {:put_entries, CustomImpl.Cache, [{:key, :value, 1_000}]}

      assert CustomImpl.Cache.cached?(:key)
      assert_received {:cached?, CustomImpl.Cache, :key}

      assert {:key, :value, 500} = CustomImpl.Cache.entry(:key)
      assert_received {:entry, CustomImpl.Cache, :key}

      assert {500, 1_000} = CustomImpl.Cache.expiry(:key)
      assert_received {:expiry, CustomImpl.Cache, :key}

      assert 1 = CustomImpl.Cache.size()
      assert_received {:size, CustomImpl.Cache}
    end

    for {name, bust, expected_kw} <- [
          {"primary key", 5, [id: 5]},
          {"keyword", [source_id: 5], [source_id: 5]}
        ] do
      test "bust_keys/1 with a #{name}" do
        assert {:ok, 1} = ContextCache.bust_keys([{CustomImpl, unquote(bust)}])
        assert_received {:bust_by, CustomImpl.Cache, unquote(expected_kw)}
      end
    end
  end

  describe "default tombstones/1 and stale_entry?/2" do
    setup do
      Cachex.clear!(Tombstones.Cache)
      :ok
    end

    for {name, pkey_or_kw, expected} <- [
          {"primary key", 1, [1]},
          {"keyword with an id", [id: 1, other: :info], [1]},
          {"keyword without an id", [user_id: 1], []},
          {"unsupported value", :not_a_pkey, []}
        ] do
      test "tombstones for a #{name}" do
        assert Sources.Cache.tombstones(unquote(pkey_or_kw)) == unquote(expected)
      end
    end

    test "entry with a primary key" do
      refute Sources.Cache.stale_entry?(:key, %{id: 1})
      refute Sources.Cache.stale_entry?(:key, [%{id: 1}, %{id: 2}])

      Tombstones.Cache.put_tombstone(Sources.Cache, 2)

      refute Sources.Cache.stale_entry?(:key, %{id: 1})
      assert Sources.Cache.stale_entry?(:key, %{id: 2})
      assert Sources.Cache.stale_entry?(:key, [%{id: 1}, %{id: 2}])
    end

    test "entry without a primary key" do
      assert Sources.Cache.stale_entry?(:key, :value)
      assert Sources.Cache.stale_entry?(:key, nil)
    end
  end

  describe "unboxed transaction" do
    setup do
      on_exit(fn ->
        SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
          for u <- Logflare.Repo.all(Logflare.User) do
            Logflare.Repo.delete(u)
          end
        end)
      end)

      :ok
    end

    test "TransactionBroadcaster subscribes to wal and broadcasts transactions" do
      ContextCache.CacheBuster.subscribe_to_transactions()
      start_supervised!({TransactionBroadcaster, interval: 100})
      :timer.sleep(200)

      SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
        insert(:user)
      end)

      :timer.sleep(500)
      assert_received %Cainophile.Changes.Transaction{}
    end
  end
end
