defmodule Logflare.Source.TextNotificationServerTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.DummyFactory

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)
    sid = s1.token
    rls = %RLS{source_id: sid}

    {:ok, sources: [s1], args: rls}
  end

  describe "GenServer" do
    test "start_link/1", %{sources: [_s1 | _], args: rls} do
      assert {:ok, _pid} = TextNotificationServer.start_link(rls)
    end

    test "init/1", %{args: rls} do
      TextNotificationServer.init(rls)
      assert_receive :check_rate, 1_100
    end
  end
end
