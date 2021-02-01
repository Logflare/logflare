defmodule Logflare.Source.TextNotificationServerTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.TextNotificationServer
  alias Logflare.Sources
  alias Logflare.Source.RecentLogsServer, as: RLS
  import Logflare.Factory

  setup do
    {:ok, u1} = Users.insert_or_update_user(params_for(:user))
    {:ok, s1} = Sources.create_source(params_for(:source), u1)
    sid = s1.token
    rls = %RLS{source_id: sid, notifications_every: 1_000}
    Sources.Counters.start_link()

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
