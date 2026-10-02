defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
  alias Logflare.ContextCache.TransactionBroadcaster

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
