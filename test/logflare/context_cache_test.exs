defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
  alias Logflare.ContextCache.TransactionBroadcaster
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Auth
  alias Logflare.Cache.CachexOps

  defmodule TestCache do
    use Logflare.ContextCache
  end

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

  describe "ContextCache" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)
      %{source: source, user: user}
    end

    test "bust_keys/1, does nothing for empty list" do
      assert {:ok, 0} = ContextCache.bust_keys([])
    end

    test "apply_fun/3,  bust_keys/1 by :id field of value", %{source: source} do
      Sources.Cache.get_by(token: source.token)
      cache_key = {:get_by, [[token: source.token]]}
      assert {:cached, %Source{}} = Cachex.get!(Sources.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Sources, source.id}])
      assert is_nil(Cachex.get!(Sources.Cache, cache_key))
    end

    test "apply_fun/3,  bust_keys/1 by :id field of value for :ok tuple", %{user: user} do
      {:ok, key} = Auth.create_access_token(user)
      assert {:ok, _token, _user} = Auth.Cache.verify_access_token(key.token)
      cache_key = {:verify_access_token, [key.token]}
      assert {:cached, {:ok, %_{}, _user}} = Cachex.get!(Auth.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Auth, key.id}])
      assert is_nil(Cachex.get!(Auth.Cache, cache_key))
    end

    test "apply_fun/3, bust_keys/1 if primary key is in list of returned structs", %{
      source: source
    } do
      backend = insert(:backend, sources: [source])
      Backends.Cache.list_backends(source_id: source.id)
      cache_key = {:list_backends, [[source_id: source.id]]}
      assert {:cached, [%Backend{}]} = Cachex.get!(Backends.Cache, cache_key)

      assert {:ok, 1} = ContextCache.bust_keys([{Backends, backend.id}])
      assert is_nil(Cachex.get!(Backends.Cache, cache_key))
    end
  end

  describe "default bust_by/1" do
    setup do
      start_supervised!(CachexOps.child_spec(TestCache, limit: nil))
      :ok
    end

    for {name, value, expected_count} <- [
          {"map with the id", %{id: 1}, 1},
          {":ok tuple with a map with the id", {:ok, %{id: 1}}, 1},
          {"list containing a map with the id", [%{id: 2}, %{id: 1}], 1},
          {"map with another id", %{id: 2}, 0},
          {"list without the id", [%{id: 2}], 0},
          {"nil", nil, 0}
        ] do
      test "cached #{name}" do
        Cachex.put(TestCache, :key, {:cached, unquote(Macro.escape(value))})

        assert {:ok, unquote(expected_count)} = TestCache.bust_by(id: 1)
        assert {:ok, unquote(expected_count == 0)} = Cachex.exists?(TestCache, :key)
      end
    end

    test "keyword other than id" do
      assert_raise ArgumentError, fn -> TestCache.bust_by(source_id: 1) end
    end
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
