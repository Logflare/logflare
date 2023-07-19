defmodule Logflare.Source.BigQuery.BufferCounterTest do
  use Logflare.DataCase, async: false

  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Source.ChannelTopics
  alias Logflare.Source.RateCounterServer
  alias Logflare.Source.RecentLogsServer

  setup do
    stub(Broadway, :push_messages, fn _, _ -> :ok end)
    stub(RateCounterServer, :should_broadcast?, fn _ -> true end)
    :ok
  end

  setup do
    source = insert(:source, user: insert(:user))
    log_events = build_list(10, :log_event, source: source, test: TestUtils.random_string())

    start_supervised!({BufferCounter, %RecentLogsServer{source_id: source.token}})

    %{source: source, log_events: log_events}
  end

  describe "init/1" do
    test "broadcasts are run with expected cadence after process start" do
      Phoenix.PubSub |> expect(:broadcast, fn _, _, _ -> :ok end)
      ChannelTopics |> expect(:broadcast_buffer, fn _ -> :ok end)
      :timer.sleep(BufferCounter.broadcast_every())
    end
  end

  describe "push_batch/1" do
    test "pushes a batch of events", %{
      source: %{token: source_token} = source,
      log_events: log_events
    } do
      batch = %{source: source, batch: log_events, count: length(log_events)}
      assert {:ok, %{len: 10, source_id: ^source_token}} = BufferCounter.push_batch(batch)
    end
  end

  describe "push/1" do
    test "pushes an event", %{source: %{token: source_token}, log_events: [log_event | _]} do
      assert {:ok, %{len: 1, source_id: ^source_token}} = BufferCounter.push(log_event)
    end
  end

  describe "ack/1" do
    test "acks messages", %{source: %{token: source_token}, log_events: [log_event | _]} do
      BufferCounter.push(log_event)

      assert {:ok, %{len: 0, source_id: ^source_token, acknowledged: 1}} =
               BufferCounter.ack(source_token, nil)
    end
  end

  describe "get_count/2" do
    test "fetches count of buffer", %{source: source, log_events: [log_event | _]} do
      BufferCounter.push(log_event)
      assert 1 = BufferCounter.get_count(source)
    end
  end

  describe "set_len_max/2" do
    test "changes process state to reflect change in len", %{source: source} do
      max = TestUtils.random_pos_integer()
      assert {:ok, %{len_max: ^max}} = BufferCounter.set_len_max(source.token, max)
    end
  end

  describe "name/1" do
    test "creates an atomized name for the given information" do
      assert BufferCounter.name(:potato) == :"potato-buffer"
      assert BufferCounter.name(123) == :"123-buffer"
    end
  end
end
