defmodule Logflare.Endpoints.PolicyCacheRolloutTest do
  use ExUnit.Case, async: false

  alias Logflare.Endpoints.Cache

  @moduletag :cluster

  setup_all do
    unless Node.alive?() do
      {_output, 0} = System.cmd(System.find_executable("epmd"), ["-daemon"])
      {:ok, _} = :net_kernel.start(:"policy_rollout_test@127.0.0.1", %{})
      on_exit(fn -> :net_kernel.stop() end)
    end

    :ok
  end

  setup do
    {:ok, peer, peer_node} =
      :peer.start(%{
        name: :policy_rollout_peer,
        host: ~c"127.0.0.1",
        args: [~c"+S", ~c"1:1"],
        env: [{~c"ERL_AFLAGS", ~c"-setcookie #{:erlang.get_cookie()}"}]
      })

    on_exit(fn -> if Process.alive?(peer), do: :peer.stop(peer) end)
    :erpc.call(peer_node, :code, :add_paths, [:code.get_path()])
    {:ok, _apps} = :erpc.call(peer_node, Application, :ensure_all_started, [:elixir])
    %{peer_node: peer_node}
  end

  test "a legacy node without the invalidation entry point has no policy cache to evict",
       context do
    :erpc.call(context.peer_node, Code, :compile_string, [
      "defmodule Logflare.Endpoints.Cache do def legacy?, do: true end"
    ])

    assert Cache.invalidate_endpoint_policy_cluster(-999) == :ok
  end

  test "an undefined function inside an upgraded invalidator remains a failure", context do
    :erpc.call(context.peer_node, Code, :compile_string, [
      "defmodule Logflare.Endpoints.Cache do " <>
        "def invalidate_endpoint_policy(_id), do: apply(__MODULE__, :missing_internal, []) end"
    ])

    assert Cache.invalidate_endpoint_policy_cluster(-999) == {:error, :cache_invalidation_failed}
  end

  test "an upgraded node reporting an eviction error remains a failure", context do
    :erpc.call(context.peer_node, Code, :compile_string, [
      "defmodule Logflare.Endpoints.Cache do " <>
        "def invalidate_endpoint_policy(_id), do: {:error, :broken_cache} end"
    ])

    assert Cache.invalidate_endpoint_policy_cluster(-999) == {:error, :cache_invalidation_failed}
  end
end
