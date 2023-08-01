defmodule Logflare.Source.BigQuery.BufferTest do
  @moduledoc false
  alias Logflare.Source.BigQuery
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.LogEvent

  use Logflare.DataCase

  doctest(BigQuery.BufferCounter)

  setup do
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)

    insert(:plan)
    user = insert(:user)

    source = insert(:source, user: user)
    rls = %RecentLogsServer{source: source, source_id: source.token}
    start_supervised!({RecentLogsServer, rls}, id: :source)

    :timer.sleep(250)
    [source: source]
  end

  test "push a log event", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    BigQuery.BufferCounter.push(le)

    assert 1 = BigQuery.BufferCounter.get_count(source)
  end

  test "ack a log event", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    BigQuery.BufferCounter.push(le)

    BigQuery.BufferCounter.ack(source.token, "some-uuid")

    assert 0 = BigQuery.BufferCounter.get_count(source)
  end

  test "ack a batch of log events", %{source: source} do
    le = LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

    BigQuery.BufferCounter.push(le)

    message = %Broadway.Message{
      data: le,
      acknowledger: {BigQuery.BufferProducer, source.token, nil}
    }

    BigQuery.BufferCounter.ack(source.token, [message])

    assert 0 = BigQuery.BufferCounter.get_count(source)
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

    assert 2 = BigQuery.BufferCounter.get_count(source)
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
  end
end
