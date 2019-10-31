defmodule Logflare.Source.RecentLogsServerTest do
  @moduledoc false
  alias Logflare.Source.RecentLogsServer, as: RLS
  alias Logflare.Sources
  alias Logflare.Source.Data
  use LogflareWeb.ChannelCase
  use Placebo
  import Logflare.Factory

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)
    Sources.Counters.start_link()

    {:ok, sources: [s1]}
  end

  describe "GenServer" do
    test "load_init_log_message/2", %{sources: [s1 | _]} do
      log_count = 1
      @endpoint.subscribe("source:#{s1.token}")

      allow Data.get_log_count(s1.token, "project-id"), return: log_count

      RLS.load_init_log_message(s1.token, "project-id")

      event = "source:#{s1.token}:new"

      assert_broadcast ^event,
                       %{
                         body: %{
                           timestamp: _,
                           message:
                             "Initialized. Waiting for new events. 1 available to explore & search."
                         }
                       },
                       1_000
    end
  end
end
