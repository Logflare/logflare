defmodule Logflare.SlackHookServerTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.Sources.Source.SlackHookServer
  alias Logflare.Backends.Adaptor.SlackAdaptor
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Sources.Counters

  setup do
    insert(:plan)
    user = insert(:user)

    source =
      insert(:source,
        user_id: user.id,
        slack_hook_url: "http://localhost:4000",
        notifications_every: 100
      )

    IngestEventQueue.upsert_tid({source.id, nil, nil})

    [source: source, user: user]
  end

  describe "GenServer" do
    test "send only recent events", %{source: source} do
      pid = self()
      ref = make_ref()

      IngestEventQueue.add_to_table({source.id, nil}, [
        build(:log_event,
          message: "old event",
          timestamp: DateTime.utc_now() |> DateTime.add(-6, :day) |> DateTime.to_string()
        ),
        build(:log_event, message: "new event"),
        build(:log_event,
          message: "old event",
          timestamp: DateTime.utc_now() |> DateTime.add(-5, :day) |> DateTime.to_string()
        )
      ])

      SlackAdaptor.Client
      |> expect(:send, 1, fn _url, payload ->
        send(pid, {ref, payload})
        {:ok, %Tesla.Env{}}
      end)

      start_supervised!({SlackHookServer, source: source})

      :timer.sleep(500)
      Counters.increment(source.token)
      Counters.increment(source.token)
      :timer.sleep(500)

      assert_receive {^ref, %{blocks: [_section, rtf]}}, 2_000
      assert inspect(rtf) =~ "new event"
      refute inspect(rtf) =~ "old event"
    end
  end
end
