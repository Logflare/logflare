defmodule Logflare.KeyValues.CacheClusterTest do
  use Logflare.DataCase, async: false

  alias Ecto.Adapters.SQL.Sandbox, as: EctoSandbox
  alias Logflare.ContextCache.CacheBusterWorker
  alias Logflare.KeyValues
  alias Logflare.KeyValues.Cache.L2
  alias Logflare.KeyValues.KeyValue
  alias Logflare.Repo
  alias Logflare.TestUtils

  @moduletag :cluster

  setup_all do
    if not Node.alive?() do
      case :net_kernel.start(:"test@127.0.0.1", %{}) do
        {:ok, _pid} ->
          on_exit(fn -> :ok = :net_kernel.stop() end)

        {:error, reason} ->
          raise "Failed to start distributed Erlang, is `epmd -daemon` running? #{inspect(reason)}"
      end
    end

    peers = Enum.map([:kv_peer_a, :kv_peer_b], &start_peer/1)
    :ok = await_ring(peers)
    [peers: peers]
  end

  setup do
    [user: unboxed(fn -> insert(:user) end), key: TestUtils.random_string()]
  end

  test "fill on one node, lookup on another", %{peers: [a, b], user: user, key: key} do
    kv = unboxed(fn -> insert(:key_value, user: user, key: key, value: %{"v" => "cached"}) end)

    ref = attach_l2_fill(a)
    assert %{"v" => "cached"} = lookup(a, user.id, key)
    assert_receive {_event, ^ref, _measurements, %{result: :put}}
    put_value(kv, %{"v" => "unbusted"})

    assert %{"v" => "cached"} = lookup(b, user.id, key)
  end

  test "change busted on every node", %{peers: [a, b] = peers, user: user, key: key} do
    kv = unboxed(fn -> insert(:key_value, user: user, key: key, value: %{"v" => "old"}) end)

    for peer <- peers, do: assert(%{"v" => "old"} = lookup(peer, user.id, key))
    put_value(kv, %{"v" => "new"})
    bust_everywhere(peers, user.id, key)

    assert %{"v" => "new"} = lookup(a, user.id, key)
    assert %{"v" => "new"} = lookup(b, user.id, key)
  end

  test "fill racing a change busted on every node", %{peers: [a, b] = peers, user: user} do
    ref = attach_l2_fill(a)

    for {stale, fresh} <- [{%{"v" => "old"}, %{"v" => "new"}}, {nil, %{"v" => "inserted"}}] do
      key = TestUtils.random_string()
      kv = stale && unboxed(fn -> insert(:key_value, user: user, key: key, value: stale) end)

      bust_everywhere(peers, user.id, key)
      assert lookup(a, user.id, key) == stale
      assert_receive {_event, ^ref, _measurements, %{result: :dropped_stale}}
      put_value(kv || %KeyValue{user_id: user.id, key: key}, fresh)

      assert lookup(b, user.id, key) == fresh
    end
  end

  defp attach_l2_fill(peer) do
    event = [:logflare, :key_values, :cache, :l2_fill, :stop]
    ref = :erpc.call(peer, :telemetry_test, :attach_event_handlers, [self(), [event]])
    on_exit(fn -> :erpc.call(peer, :telemetry, :detach, [ref]) end)
    ref
  end

  defp lookup(peer, user_id, key), do: :erpc.call(peer, KeyValues.Cache, :lookup, [user_id, key])

  defp bust_everywhere(peers, user_id, key) do
    for peer <- peers do
      {:noreply, _state} =
        :erpc.call(peer, CacheBusterWorker, :handle_cast, [
          {:to_bust, [{KeyValues, user_id: user_id, key: key}], []},
          %{}
        ])
    end
  end

  defp put_value(kv, value) do
    unboxed(fn -> kv |> Ecto.Changeset.change(value: value) |> Repo.insert_or_update!() end)
  end

  defp unboxed(fun) do
    record = EctoSandbox.unboxed_run(Repo, fun)

    on_exit(fn ->
      EctoSandbox.unboxed_run(Repo, fn -> Repo.delete(record, allow_stale: true) end)
    end)

    record
  end

  defp await_ring(peers, attempts \\ 100) do
    cond do
      Enum.all?(peers, &(Enum.sort(:erpc.call(&1, L2, :nodes, [])) == Enum.sort(peers))) ->
        :ok

      attempts == 0 ->
        {:error, :ring_not_converged}

      true ->
        Process.sleep(100)
        await_ring(peers, attempts - 1)
    end
  end

  defp start_peer(name) do
    {:ok, _peer, node} =
      :peer.start_link(%{
        name: name,
        host: ~c"127.0.0.1",
        env: [{~c"ERL_AFLAGS", ~c"-setcookie #{:erlang.get_cookie()}"}]
      })

    true = Node.connect(node)
    :erpc.call(node, :code, :add_paths, [:code.get_path()])

    for {app, _, _} <- Application.loaded_applications(),
        {key, val} <- Application.get_all_env(app) do
      :erpc.call(node, Application, :put_env, [app, key, val, [persistent: true]])
    end

    kv_cache_env = Keyword.put(Application.get_env(:logflare, KeyValues.Cache), :l2_enabled, true)

    :erpc.call(node, Application, :put_env, [
      :logflare,
      KeyValues.Cache,
      kv_cache_env,
      [persistent: true]
    ])

    :erpc.call(node, Application, :put_env, [:logflare, LogflareWeb.Endpoint, [server: false]])
    :erpc.call(node, Application, :put_env, [:logflare, :enable_cainophile, false])
    {:ok, _apps} = :erpc.call(node, Application, :ensure_all_started, [:logflare])

    node
  end
end
