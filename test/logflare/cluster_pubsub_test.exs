defmodule Logflare.ClusterPubSubTest do
  @moduledoc false
  use Logflare.DataCase, async: false

  alias Logflare.PubSubRates

  defp flush_mailbox do
    receive do
      _ -> flush_mailbox()
    after
      0 -> :ok
    end
  end

  describe "PubSubRates" do
    setup do
      on_exit(&flush_mailbox/0)
      [source: insert(:source, user: insert(:user))]
    end

    test "buffers 3-elem tuple is no op", %{source: source} do
      PubSubRates.global_broadcast_rate({"buffers", source.token, %{Node.self() => %{len: 5}}})

      :timer.sleep(100)
      assert PubSubRates.Cache.get_cluster_buffers(source.id, nil) == 0

      PubSubRates.global_broadcast_rate({"buffers", source.id, nil, %{Node.self() => %{len: 5}}})

      :timer.sleep(100)
      assert PubSubRates.Cache.get_cluster_buffers(source.id, nil) == 5
    end
  end
end
