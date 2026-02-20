defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Rules
  alias Logflare.Rules.Rule
  alias Logflare.Auth

  describe "ContextCache" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)
      %{source: source, user: user}
    end

    test "bust_keys/1, does nothing for empty list" do
      assert :ok = ContextCache.bust_keys([])
    end

    test "apply_fun/3,  bust_keys/1 by :id field of value", %{source: source} do
      Sources.Cache.get_by(token: source.token)
      cache_key = {:get_by, [[token: source.token]]}
      assert {:cached, %Source{}} = Cachex.get!(Sources.Cache, cache_key)

      size_before = Cachex.size!(Sources.Cache)
      assert :ok = ContextCache.bust_keys([{Sources, source.id}])
      assert Cachex.size!(Sources.Cache) == size_before - 1
      assert is_nil(Cachex.get!(Sources.Cache, cache_key))
    end

    test "apply_fun/3,  bust_keys/1 by :id field of value for :ok tuple", %{user: user} do
      {:ok, key} = Auth.create_access_token(user)
      assert {:ok, _token, _user} = Auth.Cache.verify_access_token(key.token)
      cache_key = {:verify_access_token, [key.token]}
      assert {:cached, {:ok, %_{}, _user}} = Cachex.get!(Auth.Cache, cache_key)

      size_before = Cachex.size!(Auth.Cache)
      assert :ok = ContextCache.bust_keys([{Auth, key.id}])
      assert Cachex.size!(Auth.Cache) == size_before - 1
      assert is_nil(Cachex.get!(Auth.Cache, cache_key))
    end

    test "apply_fun/3, bust_keys/1 if primary key is in list of returned structs", %{
      source: source
    } do
      backend = insert(:backend, sources: [source])
      Backends.Cache.list_backends(source_id: source.id)
      cache_key = {:list_backends, [[source_id: source.id]]}
      assert {:cached, [%Backend{}]} = Cachex.get!(Backends.Cache, cache_key)

      size_before = Cachex.size!(Backends.Cache)
      assert :ok = ContextCache.bust_keys([{Backends, backend.id}])
      assert Cachex.size!(Backends.Cache) == size_before - 1
      assert is_nil(Cachex.get!(Backends.Cache, cache_key))
    end

    test "refresh_keys/1 regular entry", %{source: source} do
      old_name = source.name
      new_name = "NotHotdog"
      Sources.Cache.get_by(token: source.token)
      change = Source.changeset(source, %{name: new_name})
      assert {:ok, _new_source} = Repo.update(change)

      cache_key = {:get_by, [[token: source.token]]}
      assert {:cached, %Source{name: ^old_name}} = Cachex.get!(Sources.Cache, cache_key)

      assert :ok = ContextCache.refresh_keys([{Sources, source.id}])
      assert {:cached, %Source{name: ^new_name}} = Cachex.get!(Sources.Cache, cache_key)
    end

    test "refresh_keys/1 list entry", %{source: source, user: user} do
      backend = insert(:backend, sources: [source], user: user)
      Backends.Cache.list_backends(source_id: source.id)
      old_name = backend.name
      new_name = "NotHotdog"
      change = Backend.changeset(backend, %{name: new_name})
      assert {:ok, %Backend{name: ^new_name}} = Repo.update(change)

      cache_key = {:list_backends, [[source_id: source.id]]}
      assert {:cached, [%Backend{name: ^old_name}]} = Cachex.get!(Backends.Cache, cache_key)

      assert :ok = ContextCache.refresh_keys([{Backends, backend.id}])
      assert {:cached, [%Backend{name: ^new_name}]} = Cachex.get!(Backends.Cache, cache_key)
    end

    test "refresh_keys/1 via keys_to_bust callback", %{source: source, user: user} do
      backend = insert(:backend, user: user)
      rule = insert(:rule, source: source, backend: backend)

      Rules.Cache.get_rule(rule.id)
      Rules.Cache.list_by_source_id(source.id)
      Rules.Cache.list_by_backend_id(backend.id)

      old_lql = rule.lql_string
      new_lql = "m.field:42"
      change = Rules.Rule.changeset(rule, %{lql_string: new_lql})
      assert {:ok, %Rule{lql_string: ^new_lql}} = Repo.update(change)

      cache_get_key = {:get_rule, [rule.id]}
      cache_sid_key = {:list_by_source_id, [source.id]}
      cache_bid_key = {:list_by_backend_id, [backend.id]}

      assert {:cached, %Rule{lql_string: ^old_lql}} = Cachex.get!(Rules.Cache, cache_get_key)
      assert {:cached, [%Rule{lql_string: ^old_lql}]} = Cachex.get!(Rules.Cache, cache_sid_key)
      assert {:cached, [%Rule{lql_string: ^old_lql}]} = Cachex.get!(Rules.Cache, cache_bid_key)

      assert :ok =
               ContextCache.refresh_keys([
                 {Rules, id: rule.id, source_id: source.id, backend_id: backend.id}
               ])

      assert {:cached, %Rule{lql_string: ^new_lql}} = Cachex.get!(Rules.Cache, cache_get_key)
      assert {:cached, [%Rule{lql_string: ^new_lql}]} = Cachex.get!(Rules.Cache, cache_sid_key)
      assert {:cached, [%Rule{lql_string: ^new_lql}]} = Cachex.get!(Rules.Cache, cache_bid_key)
    end
  end

  describe "unboxed transaction" do
    setup do
      on_exit(fn ->
        Ecto.Adapters.SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
          for u <- Logflare.Repo.all(Logflare.User) do
            Logflare.Repo.delete(u)
          end
        end)
      end)

      :ok
    end

    test "TransactionBroadcaster subscribes to wal and broadcasts transactions" do
      ContextCache.CacheBuster.subscribe_to_transactions()
      start_supervised!({ContextCache.TransactionBroadcaster, interval: 100})
      :timer.sleep(200)

      Ecto.Adapters.SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
        insert(:user)
      end)

      :timer.sleep(500)
      assert_received %Cainophile.Changes.Transaction{}
    end
  end
end
