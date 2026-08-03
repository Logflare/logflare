defmodule Logflare.Sources.Source.TextNotificationServerTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Sources.Source.TextNotificationServer

  setup do
    u1 = insert(:user)
    s1 = insert(:source, user_id: u1.id, notifications_every: 1_000)
    plan = insert(:plan, name: "metered")
    {:ok, source: s1, user: u1, plan: plan}
  end

  describe "GenServer" do
    test "start_link/1", %{source: source, plan: plan} do
      start_supervised!({TextNotificationServer, plan: plan, source: source})
    end

    test "init/1", %{source: source, plan: plan} do
      TextNotificationServer.init(source: source, plan: plan)
      assert_receive :check_rate, 1_100
    end
  end

  test "no message sent if on free plan" do
    ExTwilio.Message
    |> stub()

    user = insert(:user)
    plan = insert(:plan, name: "Free")

    source =
      insert(:source,
        user: user,
        notifications_every: 1_000,
        notifications: %{user_text_notifications: true}
      )

    pid =
      start_supervised!(
        {TextNotificationServer,
         [
           plan: plan,
           source: source
         ]}
      )

    send(pid, :check_rate)
  end
end
