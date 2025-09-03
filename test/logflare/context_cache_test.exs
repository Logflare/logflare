defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Backends
  alias Logflare.Backends.Backend
  alias Logflare.Auth

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

  describe "ContextCache.Supervisor" do
    setup do
      ContextCache.Supervisor.remove_cainophile()

      on_exit(fn ->
        ContextCache.Supervisor.remove_cainophile()
      end)

      :ok
    end

    test "maybe_start_cainophile will attempt to start a cainophile child" do
      assert {:ok, _pid} = ContextCache.Supervisor.maybe_start_cainophile()

      assert {:error, {:already_started, _}} =
               ContextCache.Supervisor.maybe_start_cainophile()

      assert get_cainophile_child()
    end

    test "remove_cainophile/1 will remove cainophile child from tree" do
      refute get_cainophile_child()
      assert {:ok, _pid} = ContextCache.Supervisor.maybe_start_cainophile()
      assert get_cainophile_child()
      assert :ok = ContextCache.Supervisor.remove_cainophile()
      refute get_cainophile_child()
    end

    test "TransactionBroadcaster will try to start cainophile " do
      refute get_cainophile_child()
      start_supervised!({ContextCache.TransactionBroadcaster, interval: 100})
      :timer.sleep(500)
      assert get_cainophile_child()
    end
  end

  describe "unboxed transaction" do
    setup do
      ContextCache.Supervisor.remove_cainophile()

      on_exit(fn ->
        ContextCache.Supervisor.remove_cainophile()

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

  # describe "ContextCache"

  defp get_cainophile_child do
    for {Cainophile.Adapters.Postgres, _, _, _} = child <-
          Supervisor.which_children(ContextCache.Supervisor) do
      child
    end
    |> List.first()
  end
end
