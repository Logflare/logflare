defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL
  alias Logflare.ContextCache
  alias Logflare.ContextCache.TransactionBroadcaster
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Auth
  alias Logflare.Backends
  alias Logflare.Backends.Backend

  defp cache_setup(_ctx) do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    %{source: source, user: user}
  end

  describe "ContextCache" do
    setup :cache_setup

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
  end

  describe "refresh_keys/1" do
    setup :cache_setup

    test "entry update on full keys list", %{source: source} do
      Sources.Cache.get_by(id: source.id)
      new_name = "NotHotdog"
      cache_key = {:get_by, [[id: source.id]]}
      assert {:cached, %Source{}} = Cachex.get!(Sources.Cache, cache_key)
      missing_cache_key = {:get_by, [[public_token: "invalid"]]}

      new_source = %{source | name: new_name}
      actions = %{cache_key => new_source, missing_cache_key => new_source}
      assert :ok = ContextCache.refresh_keys([{Sources, source.id, {:full, actions}}])
      assert {:cached, %Source{name: ^new_name}} = Cachex.get!(Sources.Cache, cache_key)
      assert Cachex.size!(Sources.Cache) == 1
    end

    test "handles multiple keys", %{source: source, user: user} do
      backend = insert(:backend, sources: [source], user: user)
      Backends.Cache.get_backend(backend.id)
      new_name = "NotHotdog"
      cache_key = {:get_backend, [backend.id]}
      assert {:cached, %Backend{}} = Cachex.get!(Backends.Cache, cache_key)

      new_backend = %{backend | name: new_name}
      actions = %{cache_key => new_backend, {:get_backend, [backend.id + 999]} => :bust}
      assert :ok = ContextCache.refresh_keys([{Backends, backend.id, {:full, actions}}])
      assert {:cached, %Backend{name: ^new_name}} = Cachex.get!(Backends.Cache, cache_key)
    end

    test "bust action", %{source: source} do
      Sources.Cache.get_by(id: source.id)
      cache_key = {:get_by, [[id: source.id]]}
      assert {:cached, %Source{}} = Cachex.get!(Sources.Cache, cache_key)

      actions = %{cache_key => :bust}

      assert :ok = ContextCache.refresh_keys([{Sources, source.id, {:full, actions}}])
      assert Cachex.get!(Sources.Cache, cache_key) == nil
      assert Cachex.size!(Backends.Cache) == 0
    end

    test "entry update and ETS scan on partial keys map", %{source: source, user: user} do
      backend = insert(:backend, sources: [source], user: user)
      Backends.Cache.get_backend(backend.id)
      get_key = {:get_backend, [backend.id]}
      assert {:cached, %Backend{}} = Cachex.get!(Backends.Cache, get_key)
      Backends.Cache.list_backends(source_id: source.id)
      list_key = {:list_backends, [[source_id: source.id]]}
      assert {:cached, [%Backend{}]} = Cachex.get!(Backends.Cache, list_key)

      new_name = "NotHotdog"
      new_backend = %{backend | name: new_name}
      actions = %{get_key => new_backend}
      assert :ok = ContextCache.refresh_keys([{Backends, backend.id, {:partial, actions}}])

      assert {:cached, %Backend{name: ^new_name}} = Cachex.get!(Backends.Cache, get_key)
      assert Cachex.get!(Backends.Cache, list_key) == nil
    end

    test "no refresh of missing key on partial keys map", %{source: source, user: user} do
      backend = insert(:backend, sources: [source], user: user)
      # Only cache list_backends, NOT get_backend
      Backends.Cache.list_backends(source_id: source.id)
      list_key = {:list_backends, [[source_id: source.id]]}
      get_key = {:get_backend, [backend.id]}
      assert {:cached, [%Backend{}]} = Cachex.get!(Backends.Cache, list_key)
      assert Cachex.get!(Backends.Cache, get_key) == nil

      actions = %{get_key => backend}
      assert :ok = ContextCache.refresh_keys([{Backends, backend.id, {:partial, actions}}])

      # list_key should be busted by scan
      # get_key was NOT present before — must not be inserted
      assert Cachex.get!(Backends.Cache, get_key) == nil
      assert Cachex.get!(Backends.Cache, list_key) == nil
      assert Cachex.size!(Backends.Cache) == 0
    end

    test "replaces a cached list value", %{source: source, user: user} do
      backend = insert(:backend, sources: [source], user: user)
      Backends.Cache.list_backends(source_id: source.id)
      old_name = backend.name
      new_name = "NotHotdog"
      cache_key = {:list_backends, [[source_id: source.id]]}
      assert {:cached, [%Backend{name: ^old_name}]} = Cachex.get!(Backends.Cache, cache_key)

      updated_backend = %{backend | name: new_name}
      actions = %{cache_key => [updated_backend]}
      assert :ok = ContextCache.refresh_keys([{Backends, backend.id, {:full, actions}}])
      assert {:cached, [%Backend{name: ^new_name}]} = Cachex.get!(Backends.Cache, cache_key)
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

    test "TransactionBroadcaster subscribes to wal and broadcasts cache_updates" do
      ContextCache.CacheBuster.subscribe_updates()
      start_supervised!({TransactionBroadcaster, interval: 100})
      :timer.sleep(200)

      SQL.Sandbox.unboxed_run(Logflare.Repo, fn ->
        insert(:user)
      end)

      assert_receive {:cache_updates, _results}, 500
    end
  end
end
