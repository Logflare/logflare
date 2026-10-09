defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.Cache.CachexOps
  alias Logflare.ContextCache
  alias Logflare.ContextCache.TransactionBroadcaster

  defmodule Recording.Cache do
    @behaviour Logflare.ContextCache

    @impl true
    def keys_to_bust(kw), do: record({:keys_to_bust, kw}, [:key])
    @impl true
    def delete_keys(keys), do: record({:delete_keys, keys}, {:ok, 1})
    @impl true
    def fetch(key, _getter), do: record({:fetch, key}, :fetched)
    @impl true
    def update(key, value), do: record({:update, key, value}, :ok)

    defp record(call, result) do
      send(self(), call)
      result
    end
  end

  defmodule Default do
    def get(id), do: %{id: id}
  end

  defmodule Default.Cache do
    @behaviour Logflare.ContextCache
  end

  test "bust_keys/1 with an empty list" do
    assert {:ok, 0} = ContextCache.bust_keys([])
  end

  describe "context cache recording its callback calls" do
    test "apply_fun/3" do
      assert :fetched = ContextCache.apply_fun(Recording, :get, [1])
      assert_received {:fetch, {:get, [1]}}
    end

    test "update/4" do
      assert :ok = ContextCache.update(Recording, :get, [1], :value)
      assert_received {:update, {:get, [1]}, :value}
    end

    for {name, bust, expected_kw} <- [
          {"primary key", 5, [id: 5]},
          {"keyword", [source_id: 5], [source_id: 5]}
        ] do
      test "bust_keys/1 with a #{name}" do
        assert {:ok, 1} = ContextCache.bust_keys([{Recording, unquote(bust)}])
        assert_received {:keys_to_bust, unquote(expected_kw)}
        assert_received {:delete_keys, [:key]}
      end
    end
  end

  test "context cache without callbacks" do
    start_supervised!(CachexOps.child_spec(Default.Cache, limit: nil))

    assert %{id: 1} = ContextCache.apply_fun(Default, :get, [1])
    assert :ok = ContextCache.update(Default, :get, [1], %{id: 1, updated: true})
    assert %{updated: true} = ContextCache.apply_fun(Default, :get, [1])
    assert {:ok, 1} = ContextCache.bust_keys([{Default, 1}])
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
