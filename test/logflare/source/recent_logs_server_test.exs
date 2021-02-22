defmodule Logflare.Source.RecentLogsServerTest do
  @moduledoc false
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Source.Data
  use Logflare.Commons
  use LogflareWeb.ChannelCase
  import Phoenix.ChannelTest
  use Placebo
  import Logflare.Factory

  setup do
    {:ok, u1} = Users.insert_or_update_user(params_for(:user))
    {:ok, s1} = Sources.create_source(params_for(:source), u1)
    Sources.Counters.start_link()

    {:ok, sources: [s1]}
  end

  describe "GenServer" do
    test "load_init_log_message/2", %{sources: [s1 | _]} do
      log_count = 1
      Phoenix.PubSub.subscribe(Logflare.PubSub, "source:#{s1.token}")

      allow Data.get_log_count(s1.token, "project-id"), return: log_count

      RLS.load_init_log_message(s1.token)

      event = "source:#{s1.token}:new"

      msg =
        "Initialized on node #{Node.self()}. Waiting for new events. Send some logs, then try to explore & search!"

      assert_broadcast ^event,
                       %{
                         body: %{
                           timestamp: _,
                           message: ^msg
                         }
                       },
                       2_000
    end
  end
end
