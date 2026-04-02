defmodule Logflare.ContextCache.GossipClusterTest do
  use Logflare.DataCase, async: false
  import Logflare.Factory

  alias Ecto.Adapters.SQL.Sandbox, as: EctoSandbox
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Sources

  @moduletag :cluster

  setup_all do
    System.cmd("epmd", ["-daemon"])

    if not Node.alive?() do
      {:ok, _} = :net_kernel.start([:"primary@127.0.0.1"])
    end

    :ok
  end

  setup do
    prev = Application.get_env(:logflare, :context_cache_gossip)

    Application.put_env(:logflare, :context_cache_gossip, %{
      enabled: true,
      ratio: 1.0,
      max_nodes: 5
    })

    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :context_cache_gossip, :receive, :stop]
      ])

    on_exit(fn ->
      Application.put_env(:logflare, :context_cache_gossip, prev)
      :telemetry.detach(telemetry_ref)
    end)

    {plan, user, source} =
      EctoSandbox.unboxed_run(Logflare.Repo, fn ->
        plan = insert(:plan, name: "Free")
        user = insert(:user)
        source = insert(:source, user: user)
        {plan, user, source}
      end)

    on_exit(fn ->
      EctoSandbox.unboxed_run(Logflare.Repo, fn ->
        Logflare.Repo.delete(source)
        Logflare.Repo.delete(user)
        Logflare.Repo.delete(plan)
      end)
    end)

    {:ok, telemetry_ref: telemetry_ref, source: source, user: user}
  end

  @tag :capture_log
  test "cache miss on peer gossips to local node", %{telemetry_ref: telemetry_ref, source: source} do
    peer = start_peer()

    Cachex.clear!(Sources.Cache)
    :erpc.call(peer, Cachex, :clear!, [Sources.Cache])

    # trigger cache miss on the peer
    :erpc.call(peer, Sources.Cache, :get_by, [[token: source.token]])

    # wait for the local telemetry event indicating gossip was received
    assert_receive {[:logflare, :context_cache_gossip, :receive, :stop], ^telemetry_ref,
                    _measurements, %{cache: Sources.Cache, action: :cached}},
                   to_timeout(second: 10)

    assert {:cached, %Sources.Source{id: id}} =
             Cachex.get!(Sources.Cache, {:get_by, [[token: source.token]]})

    assert id == source.id
  end

  @tag :capture_log
  test "local node drops peer gossip if record is tombstoned", %{
    telemetry_ref: telemetry_ref,
    source: source
  } do
    peer = start_peer()

    Cachex.clear!(Sources.Cache)
    :erpc.call(peer, Cachex, :clear!, [Sources.Cache])

    # write tombstone LOCALLY
    Tombstones.Cache.put_tombstone({Sources.Cache, source.id})

    # trigger cache miss ON THE PEER
    :erpc.call(peer, Sources.Cache, :get_by, [[token: source.token]])

    # wait for the local telemetry event
    assert_receive {[:logflare, :context_cache_gossip, :receive, :stop], ^telemetry_ref,
                    _measurements, %{cache: Sources.Cache, action: :dropped_stale}},
                   to_timeout(second: 10)

    refute Cachex.get!(Sources.Cache, {:get_by, [[token: source.token]]})
  end

  defp start_peer(name \\ :peer) do
    {:ok, _peer, node} =
      :peer.start_link(%{
        name: name,
        host: ~c"127.0.0.1",
        env: [{~c"ERL_AFLAGS", ~c"-setcookie #{:erlang.get_cookie()}"}]
      })

    true = Node.connect(node)

    :ok = :erpc.call(node, :code, :add_paths, [:code.get_path()])

    for {app_name, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app_name) do
        :ok = :erpc.call(node, Application, :put_env, [app_name, key, val])
      end
    end

    :ok =
      :erpc.call(node, Application, :put_env, [:logflare, LogflareWeb.Endpoint, [server: false]])

    {:ok, _} = :erpc.call(node, Application, :ensure_all_started, [:logflare])

    node
  end
end
