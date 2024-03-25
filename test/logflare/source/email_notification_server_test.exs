defmodule Logflare.Source.EmailNotificationServerTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.EmailNotificationServer
  alias Logflare.Source.RecentLogsServer, as: RLS

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id)
    sid = s1.token
    rls = %RLS{source_id: sid, notifications_every: 1_000}
    {:ok, sources: [s1], args: rls}
  end

  describe "GenServer" do
    test "start_link/1", %{sources: [_s1 | _], args: rls} do
      assert {:ok, _pid} = EmailNotificationServer.start_link(rls)
    end

    test "init/1", %{args: rls} do
      EmailNotificationServer.init(rls)
      assert_receive :check_rate, 1_100
    end
  end
end
