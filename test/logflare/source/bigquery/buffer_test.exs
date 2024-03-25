defmodule Logflare.Source.BigQuery.BufferTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery
  alias Logflare.Source.RecentLogsServer
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.LogEvent
  alias Logflare.Source.V1SourceSup

  setup do
    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)

    source = insert(:source, user: user)
    rls = %RecentLogsServer{source: source, source_id: source.token}
    start_supervised!({V1SourceSup, rls}, id: :source)

    [source: source]
  end

  test "push a log event", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    BigQuery.BufferCounter.push(le)

    assert 1 = BigQuery.BufferCounter.len(source)
    :timer.sleep(2000)
  end

  test "ack a log event", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    BigQuery.BufferCounter.push(le)

    BigQuery.BufferCounter.ack(source.token, "some-uuid")

    assert 0 = BigQuery.BufferCounter.len(source)
    :timer.sleep(2000)
  end

  test "ack a batch of log events", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    BigQuery.BufferCounter.push(le)

    message = %Broadway.Message{
      data: le,
      acknowledger: {BigQuery.BufferProducer, source.token, nil}
    }

    BigQuery.BufferCounter.ack_batch(source.token, [message])

    assert 0 = BigQuery.BufferCounter.len(source)
    :timer.sleep(2000)
  end

  test "push a batch of log events", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    batch = [
      %Broadway.Message{
        data: le,
        acknowledger: {BigQuery.BufferProducer, source.token, nil}
      },
      %Broadway.Message{
        data: le,
        acknowledger: {BigQuery.BufferProducer, source.token, nil}
      }
    ]

    BigQuery.BufferCounter.push_batch(%{source: source, batch: batch, count: 2})

    assert 2 = BigQuery.BufferCounter.len(source)
    :timer.sleep(2000)
  end

  test "errors when buffer is full", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    batch = [
      %Broadway.Message{
        data: le,
        acknowledger: {BigQuery.BufferProducer, source.token, nil}
      },
      %Broadway.Message{
        data: le,
        acknowledger: {BigQuery.BufferProducer, source.token, nil}
      }
    ]

    {:ok, %{len_max: 3}} = BigQuery.BufferCounter.set_len_max(source.token, 3)

    {:ok, %{len: 2}} =
      BigQuery.BufferCounter.push_batch(%{source: source, batch: batch, count: 2})

    {:ok, %{len: 4}} =
      BigQuery.BufferCounter.push_batch(%{source: source, batch: batch, count: 2})

    {:error, :buffer_full} =
      BigQuery.BufferCounter.push_batch(%{source: source, batch: batch, count: 2})

    assert %{len: 4, discarded: 2} = BigQuery.BufferCounter.get_counts(source.token)
    :timer.sleep(2000)
  end
end
