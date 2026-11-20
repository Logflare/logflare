defmodule Logflare.ContextCache.RefreshAhead.DistributedTest do
  use Logflare.DataCase, async: false

  alias Logflare.ContextCache.RefreshAhead
  alias Logflare.ContextCache.Tombstones
  alias Logflare.Sources

  @moduletag :cluster

  @event [:logflare, :context_cache, :refresh_ahead]
  @key {:refresh_ahead_distributed_test, [1]}

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

  setup %{peer: peer} do
    original_config = Application.fetch_env!(:logflare, RefreshAhead)

    Application.put_env(
      :logflare,
      RefreshAhead,
      Keyword.merge(original_config, enabled: true, threshold: 0.5, interval: 10)
    )

    reset_caches(peer)
    telemetry_ref = :telemetry_test.attach_event_handlers(self(), [@event])

    on_exit(fn ->
      Application.put_env(:logflare, RefreshAhead, original_config)
      :telemetry.detach(telemetry_ref)
      reset_caches(peer)
    end)

    [telemetry_ref: telemetry_ref]
  end

  test "copies a fresher entry from a peer instead of calling the getter", %{
    peer: peer,
    telemetry_ref: ref
  } do
    :ok =
      :erpc.call(peer, Sources.Cache, :put_entries, [[{@key, %{id: 1, name: "peer"}, 60_000}]])

    put_expiring(%{id: 1, name: "stale"})

    assert %{name: "stale"} = Sources.Cache.fetch(@key, notifying_getter(%{id: 1, name: "db"}))

    assert_receive {@event, ^ref, _measurements, %{result: :copied}}
    refute_received :getter_called
    assert {_key, %{name: "peer"}, ttl} = Sources.Cache.entry(@key)
    assert ttl > 50_000 and ttl <= 60_000
  end

  test "reloads from the getter when the peer's entry is about to expire too", %{
    peer: peer,
    telemetry_ref: ref
  } do
    :ok = :erpc.call(peer, Sources.Cache, :put_entries, [[{@key, %{id: 1, name: "peer"}, 1_000}]])
    put_expiring(%{id: 1, name: "stale"})

    Sources.Cache.fetch(@key, notifying_getter(%{id: 1, name: "db"}))

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}
    assert_received :getter_called
    assert {_key, %{name: "db"}, _ttl} = Sources.Cache.entry(@key)
  end

  test "reloads from the getter when the peer's entry is stale", %{
    peer: peer,
    telemetry_ref: ref
  } do
    :ok =
      :erpc.call(peer, Sources.Cache, :put_entries, [[{@key, %{id: 1, name: "peer"}, 60_000}]])

    Tombstones.Cache.put_tombstone(Sources.Cache, 1)
    put_expiring(%{id: 1, name: "stale"})

    Sources.Cache.fetch(@key, notifying_getter(%{id: 1, name: "db"}))

    assert_receive {@event, ^ref, _measurements, %{result: :refreshed}}
    assert {_key, %{name: "db"}, _ttl} = Sources.Cache.entry(@key)
  end

  defp put_expiring(value) do
    Sources.Cache.put_entries([{@key, value, 1_000}])
    Process.sleep(600)
  end

  defp notifying_getter(value) do
    test_pid = self()

    fn ->
      send(test_pid, :getter_called)
      value
    end
  end

  defp reset_caches(peer) do
    :ets.delete_all_objects(RefreshAhead)
    Sources.Cache.reset()
    Cachex.clear!(Tombstones.Cache)
    :ok = :erpc.call(peer, Sources.Cache, :reset, [])
  end

  defp start_peer do
    {:ok, _peer, node} =
      :peer.start_link(%{
        name: :refresh_ahead_peer,
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
end
