defmodule Logflare.ContextCache.GossipClusterTest do
  use Logflare.DataCase, async: false
  import Logflare.Factory

  alias Ecto.Adapters.SQL.Sandbox, as: EctoSandbox
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Sources

  @moduletag :cluster

  setup_all do
    if not Node.alive?() do
      case :net_kernel.start(:"test@127.0.0.1", %{}) do
        {:ok, _} ->
          :ok

        {:error, reason} ->
          raise """
          =============================================
          Failed to start distributed Erlang for tests.

          Please make sure `epmd` is running:

              epmd -daemon

          =============================================
          Underlying error: #{inspect(reason)}
          """
      end
    end

    original_context_cache_gossip = Application.get_env(:logflare, :context_cache_gossip)

    Application.put_env(
      :logflare,
      :context_cache_gossip,
      _guaranteed_hit = %{enabled: true, ratio: 1.0, max_nodes: 5}
    )

    on_exit(fn ->
      Application.put_env(:logflare, :context_cache_gossip, original_context_cache_gossip)
    end)

    :ok
  end

  setup do
    peer = start_peer()

    Cachex.clear!(Sources.Cache)
    :erpc.call(peer, Cachex, :clear!, [Sources.Cache])

    unboxed_insert_then_delete_on_exit(:plan, name: "Free")
    user = unboxed_insert_then_delete_on_exit(:user)
    source = unboxed_insert_then_delete_on_exit(:source, user: user)

    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :context_cache_gossip, :receive, :stop]
      ])

    on_exit(fn -> :telemetry.detach(telemetry_ref) end)

    {:ok, peer: peer, source: source, telemetry_ref: telemetry_ref}
  end

  test "cache miss on peer gossips to local node", %{
    peer: peer,
    source: source,
    telemetry_ref: telemetry_ref
  } do
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

  test "local node drops peer gossip if record is tombstoned", %{
    peer: peer,
    source: source,
    telemetry_ref: telemetry_ref
  } do
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

    :erpc.call(node, :code, :add_paths, [:code.get_path()])

    for {app, _, _} <- Application.loaded_applications() do
      for {key, val} <- Application.get_all_env(app) do
        :erpc.call(node, Application, :put_env, [app, key, val, [persistent: true]])
      end
    end

    :erpc.call(node, Application, :put_env, [:logflare, LogflareWeb.Endpoint, [server: false]])
    :erpc.call(node, Application, :put_env, [:logflare, :enable_cainophile, false])
    :erpc.call(node, Application, :ensure_all_started, [:logflare])

    node
  end

  defp unboxed_insert_then_delete_on_exit(kind, options \\ []) do
    record =
      EctoSandbox.unboxed_run(Logflare.Repo, fn ->
        insert(kind, options)
      end)

    on_exit(fn ->
      EctoSandbox.unboxed_run(Logflare.Repo, fn ->
        Logflare.Repo.delete(record)
      end)
    end)

    record
  end
end
