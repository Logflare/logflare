defmodule Logflare.ContextCacheTest do
  use Logflare.DataCase, async: false
  alias Logflare.ContextCache
  alias Logflare.Sources

  describe "ContextCache functions" do
    setup do
      user = insert(:user)
      insert(:plan, name: "Free")
      source = insert(:source, user: user)
      args = [token: source.token]
      source = Sources.Cache.get_by(args)
      fun = :get_by
      cache_key = {fun, [args]}
      %{source: source, cache_key: cache_key}
    end

    test "cache_name/1" do
      assert Sources.Cache == ContextCache.cache_name(Sources)
    end

    test "apply_fun/3", %{cache_key: cache_key} do
      # apply_fun was called in the setup when we called `Sources.Cache.get_by/1`
      # here's we're making sure it did get cached correctly
      assert {:cached, %Logflare.Source{}} = Cachex.get!(Sources.Cache, cache_key)
    end

    test "bust_keys/1", %{source: source, cache_key: cache_key} do
      assert {:ok, :busted} = ContextCache.bust_keys([{Sources, source.id}])
      assert is_nil(Cachex.get!(Sources.Cache, cache_key))
      match = {:entry, {{Sources, source.id}, :_}, :_, :_, :"$1"}
      assert [] = :ets.match(ContextCache, match)
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

  defp get_cainophile_child() do
    for {Cainophile.Adapters.Postgres, _, _, _} = child <-
          Supervisor.which_children(ContextCache.Supervisor) do
      child
    end
    |> List.first()
  end
end
