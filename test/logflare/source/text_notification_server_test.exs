defmodule Logflare.Source.TextNotificationServerTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.TextNotificationServer
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
      assert {:ok, _pid} = TextNotificationServer.start_link(rls)
    end

    test "init/1", %{args: rls} do
      TextNotificationServer.init(rls)
      assert_receive :check_rate, 1_100
    end
  end

  test "no message sent if on free plan" do
    ExTwilio.Message
    |> stub()

    user = insert(:user)
    plan = insert(:plan, name: "Free")
    source = insert(:source, user: user, notifications: %{user_text_notifications: true})

    rls = %RLS{
      plan: plan,
      source_id: source.token,
      notifications_every: 10_000,
      inserts_since_boot: 5
    }

    pid = start_supervised!({TextNotificationServer, rls})

    send(pid, :check_rate)
  end
end
