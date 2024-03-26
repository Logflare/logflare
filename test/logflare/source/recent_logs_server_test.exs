defmodule Logflare.Source.RecentLogsServerTest do
  @moduledoc false
  use LogflareWeb.ChannelCase
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Phoenix.ChannelTest
  @moduletag :failing
  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)
    {:ok, sources: [s1]}
  end

  describe "GenServer" do
    test "load_init_log_message/2", %{sources: [s1 | _]} do
      # log_count = 1
      Phoenix.PubSub.subscribe(Logflare.PubSub, "source:#{s1.token}")

      # allow Data.get_log_count(s1.token, "project-id"), return: log_count

      RLS.load_init_log_message(s1.token)

      event = "source:#{s1.token}:new"

      msg =
        "Initialized on node #{Node.self()}. Waiting for new events. Send some logs, then try to explore & search!"

      assert_broadcast(
        ^event,
        %{
          body: %{
            timestamp: _,
            message: ^msg
          }
        },
        2_000
      )
    end
  end
end
