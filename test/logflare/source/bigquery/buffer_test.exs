defmodule Logflare.Source.BufferCounterTest do
  @moduledoc false
  use Logflare.DataCase
  alias Logflare.Source.BigQuery
  alias Logflare.Source.BigQuery.BufferCounter
  alias Logflare.Source.RecentLogsServer
  alias Logflare.Sources.Counters
  alias Logflare.Sources.RateCounters
  alias Logflare.Sources.BufferCounters
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.LogEvent
  alias Logflare.Source.BigQuery.Pipeline
  alias Logflare.Source.BigQuery.Schema
  # stubs
  setup do
    Goth
    |> stub(:fetch, fn _mod -> {:ok, %Goth.Token{token: "auth-token"}} end)

    Schema
    |> stub(:update, fn _token, _le -> :ok end)

    GoogleApi.BigQuery.V2.Api.Tabledata
    |> stub(:bigquery_tabledata_insert_all, fn _conn,
                                               _project_id,
                                               _dataset_id,
                                               _table_name,
                                               _opts ->
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    Logflare.Google.BigQuery
    |> stub(:init_table!, fn _, _, _, _, _, _ -> :ok end)

    start_supervised!(AllLogsLogged)
    start_supervised!(Counters)
    start_supervised!(RateCounters)
    start_supervised!(BufferCounters)

    insert(:plan)

    :ok
  end

  describe "with expected processes" do
    setup do
      user = insert(:user)

      source = insert(:source, user: user)
      rls = %RecentLogsServer{source: source, source_id: source.token}

      start_supervised!({Pipeline, rls})
      start_supervised!({BufferCounter, rls})

      [source: source]
    end

    test "push a log event", %{source: source} do
      le =
        LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

      BufferCounter.push(le)

      assert 1 = BufferCounter.len(source)
    end

    test "ack a log event", %{source: source} do
      le =
        LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

      BufferCounter.push(le)

      message = %Broadway.Message{
        data: le,
        acknowledger: {BigQuery.BufferProducer, source.token, nil}
      }

      BufferCounter.ack_batch(source.token, [message])

      assert 0 = BufferCounter.len(source)
    end

    test "ack a batch of log events", %{source: source} do
      le =
        LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

      BufferCounter.push(le)

      message = %Broadway.Message{
        data: le,
        acknowledger: {BigQuery.BufferProducer, source.token, nil}
      }

      BufferCounter.ack_batch(source.token, [message])

      assert 0 = BufferCounter.len(source)
    end

    test "push a batch of log events", %{source: source} do
      le =
        LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

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

      BufferCounter.push_batch(source, batch)

      assert 2 = BufferCounter.len(source)
    end

    test "errors when buffer is full", %{source: source} do
      le =
        LogEvent.make(%{"event_message" => "any", "metadata" => "some_value"}, %{source: source})

      BufferCounter.set_len(source.token, 4999)

      big_batch =
        for _ <- 1..20 do
          %Broadway.Message{
            data: le,
            acknowledger: {BigQuery.BufferProducer, source.token, nil}
          }
        end

      assert :ok = BufferCounter.push_batch(source, big_batch)
      assert {:error, :buffer_full} = BufferCounter.push_batch(source, big_batch)
    end
  end
end
