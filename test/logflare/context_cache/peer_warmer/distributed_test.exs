defmodule Logflare.ContextCache.PeerWarmer.DistributedTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL.Sandbox, as: EctoSandbox
  alias Logflare.ContextCache.PeerWarmer
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L1
  alias Logflare.Sources

  @moduletag :cluster

  setup_all do
    if not Node.alive?() do
      case :net_kernel.start(:"test@127.0.0.1", %{}) do
        {:ok, _pid} ->
          on_exit(fn -> :ok = :net_kernel.stop() end)

        {:error, reason} ->
          raise "Failed to start distributed Erlang, make sure `epmd -daemon` is running: #{inspect(reason)}"
      end
    end

    [peer: start_peer()]
  end

  setup do
    original_config = Application.fetch_env!(:logflare, PeerWarmer)

    Application.put_env(
      :logflare,
      PeerWarmer,
      Keyword.merge(original_config, enabled: true, peer_wait: 100)
    )

    Cachex.clear!(Sources.Cache)
    L1.delete_all!()

    on_exit(fn ->
      Application.put_env(:logflare, PeerWarmer, original_config)
      Cachex.clear!(Sources.Cache)
      L1.delete_all!()
    end)

    :ok
  end

  test "copies Sources entries cached on the peer", %{peer: peer} do
    unboxed_insert_then_delete_on_exit(:plan)
    user = unboxed_insert_then_delete_on_exit(:user)
    source = unboxed_insert_then_delete_on_exit(:source, user: user)
    cache_key = {:get_by, [[id: source.id]]}

    assert %{id: source_id} = :erpc.call(peer, Sources.Cache, :get_by, [[id: source.id]])
    assert %{state: :ready} = :erpc.call(peer, PeerWarmer, :status, [Sources.Cache])
    assert Cachex.get!(Sources.Cache, cache_key) == nil

    assert {:ok, %{node: ^peer, count: count}} = PeerWarmer.copy_from_peer(Sources.Cache)
    assert count >= 1
    assert {:cached, %{id: ^source_id}} = Cachex.get!(Sources.Cache, cache_key)
  end

  test "copies KeyValues entries cached on the peer", %{peer: peer} do
    user = unboxed_insert_then_delete_on_exit(:user)
    unboxed_insert_then_delete_on_exit(:key_value, user: user, key: "k", value: %{"v" => 1})
    cache_key = {:lookup, [user.id, "k", nil]}

    assert %{"v" => 1} = :erpc.call(peer, KeyValues.Cache, :lookup, [user.id, "k"])
    assert %{state: :ready} = :erpc.call(peer, PeerWarmer, :status, [KeyValues.Cache])

    assert {:ok, %{node: ^peer, count: count}} = PeerWarmer.copy_from_peer(KeyValues.Cache)
    assert count >= 1
    assert {:ok, %{"v" => 1}} = L1.fetch(cache_key)
  end

  defp start_peer do
    {:ok, _peer, node} =
      :peer.start_link(%{
        name: :peer_warmer_peer,
        host: ~c"127.0.0.1",
        env: [{~c"ERL_AFLAGS", ~c"-setcookie #{:erlang.get_cookie()}"}]
      })

    true = Node.connect(node)

    :erpc.call(node, :code, :add_paths, [:code.get_path()])

    for {app, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app) do
        :erpc.call(node, Application, :put_env, [app, key, val, [persistent: true]])
      end
    end

    :erpc.call(node, Application, :put_env, [:logflare, LogflareWeb.Endpoint, [server: false]])
    :erpc.call(node, Application, :put_env, [:logflare, :enable_cainophile, false])

    :erpc.call(node, Application, :put_env, [
      :logflare,
      :context_cache_gossip,
      %{enabled: false, ratio: 0.0, max_nodes: 1},
      [persistent: true]
    ])

    :erpc.call(node, Application, :ensure_all_started, [:logflare])

    node
  end

  defp unboxed_insert_then_delete_on_exit(kind, options \\ []) do
    record = EctoSandbox.unboxed_run(Logflare.Repo, fn -> insert(kind, options) end)

    on_exit(fn ->
      EctoSandbox.unboxed_run(Logflare.Repo, fn -> Logflare.Repo.delete(record) end)
    end)

    record
  end
end
