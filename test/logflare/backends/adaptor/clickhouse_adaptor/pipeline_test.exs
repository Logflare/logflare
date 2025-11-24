defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.Pipeline

  setup do
    insert(:plan, name: "Free")

    {source, backend, cleanup_fn} = setup_clickhouse_test()
    on_exit(cleanup_fn)

    {:ok, supervisor_pid} = ClickhouseAdaptor.start_link({source, backend})

    on_exit(fn ->
      if Process.alive?(supervisor_pid) do
        Process.exit(supervisor_pid, :shutdown)
      end
    end)

    Process.sleep(200)

    adaptor_state = %{
      source: source,
      backend: backend
    }

    context = %{
      source_id: source.id,
      source_token: source.token,
      backend_id: backend.id
    }

    [
      source: source,
      backend: backend,
      adaptor_state: adaptor_state,
      context: context
    ]
  end

  describe "child_spec/1" do
    test "returns proper child specification" do
      spec = Pipeline.child_spec(:some_arg)

      assert spec.id == Pipeline
      assert spec.start == {Pipeline, :start_link, [:some_arg]}
    end
  end

  describe "process_name/2" do
    test "appends base_name to via tuple identifier" do
      via_tuple = {:via, Registry, {SomeRegistry, {1, 2, 3}}}
      base_name = :pipeline

      result = Pipeline.process_name(via_tuple, base_name)

      assert result == {:via, Registry, {SomeRegistry, {1, 2, 3, :pipeline}}}
    end
  end

  describe "handle_message/3" do
    test "routes all messages to :ch batcher", %{adaptor_state: adaptor_state} do
      log_event = build(:log_event)

      message = %Message{
        data: log_event,
        acknowledger: {Pipeline, :ack_id, :ack_data}
      }

      result = Pipeline.handle_message(:default, message, adaptor_state)

      assert %Message{batcher: :ch} = result
      assert result.data == log_event
    end
  end

  describe "handle_batch/4" do
    test "extracts events from messages and inserts into ClickHouse", %{
      context: context,
      source: source,
      backend: backend
    } do
      log_event1 = build(:log_event, source: source, message: "Test message 1")
      log_event2 = build(:log_event, source: source, message: "Test message 2")

      messages = [
        %Message{data: log_event1, acknowledger: {Pipeline, :ack_id, :ack_data}},
        %Message{data: log_event2, acknowledger: {Pipeline, :ack_id, :ack_data}}
      ]

      result = Pipeline.handle_batch(:ch, messages, %{size: 2, trigger: :flush}, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

      {:ok, query_result} =
        ClickhouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      [first_row] = query_result
      assert %{"count" => 2} = first_row
    end

    test "handles empty messages list", %{context: context} do
      result = Pipeline.handle_batch(:ch, [], %{size: 0, trigger: :flush}, context)
      assert result == []
    end

    test "handles log events with different field types", %{
      context: context,
      source: source,
      backend: backend
    } do
      log_event1 =
        build(:log_event,
          source: source,
          message: "Some message",
          metadata: %{
            "level" => "info",
            "user_id" => 123,
            "active" => true,
            "score" => 95.5
          }
        )

      log_event2 =
        build(:log_event,
          source: source,
          message: "Another message",
          metadata: %{
            "level" => "error",
            "user_id" => 456,
            "active" => false,
            "score" => 72.3
          }
        )

      messages = [
        %Message{data: log_event1, acknowledger: {Pipeline, :ack_id, :ack_data}},
        %Message{data: log_event2, acknowledger: {Pipeline, :ack_id, :ack_data}}
      ]

      result = Pipeline.handle_batch(:ch, messages, %{size: 2, trigger: :flush}, context)
      assert result == messages

      Process.sleep(200)

      table_name = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

      {:ok, query_result} =
        ClickhouseAdaptor.execute_ch_query(
          backend,
          "SELECT body FROM #{table_name} ORDER BY timestamp DESC"
        )

      assert length(query_result) == 2

      query_result = Enum.map(query_result, &Jason.decode!(&1["body"]))

      [first_row, second_row] = query_result
      assert first_row["event_message"] == "Another message"
      assert second_row["event_message"] == "Some message"
    end
  end

  describe "transform/2" do
    test "transforms event into Broadway message with correct acknowledger", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, message: "Test message")
      opts = [source_id: source.id, backend_id: backend.id]

      result = Pipeline.transform(event, opts)

      assert %Message{
               data: ^event,
               acknowledger: {Pipeline, :ack_id, %{source_id: source_id, backend_id: backend_id}}
             } = result

      assert is_integer(source_id)
      assert is_integer(backend_id)
      assert source_id == source.id
      assert backend_id == backend.id
    end
  end

  describe "ack/3" do
    test "returns :ok when failed list is empty" do
      assert Pipeline.ack(:ack_ref, [], []) == :ok
    end

    test "re-queues failed messages when `LogEvent` retries are under limit", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)

      failed_message = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, %{source_id: source.id, backend_id: backend.id}},
        status: {:failed, "connection error"}
      }

      test_pid = self()
      expected_source_id = source.id
      expected_backend_id = backend.id

      Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, fn {sid, bid}, events ->
        send(test_pid, {:deleted_batch, sid, bid, events})
        :ok
      end)

      Mimic.expect(Logflare.Backends.IngestEventQueue, :add_to_table, fn {sid, bid}, events ->
        send(test_pid, {:requeued, sid, bid, events})
        :ok
      end)

      Pipeline.ack(:ack_ref, [], [failed_message])

      assert_receive {:deleted_batch, ^expected_source_id, ^expected_backend_id, [_deleted_event]}
      assert_receive {:requeued, ^expected_source_id, ^expected_backend_id, [requeued_event]}
      assert requeued_event.retries == 1
    end

    test "increments retry count on each re-queue", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 2)

      failed_message = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, %{source_id: source.id, backend_id: backend.id}},
        status: {:failed, "connection error"}
      }

      test_pid = self()

      Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, fn {_sid, _bid}, _events ->
        :ok
      end)

      Mimic.expect(Logflare.Backends.IngestEventQueue, :add_to_table, fn {_sid, _bid}, events ->
        send(test_pid, {:requeued, events})
        :ok
      end)

      Pipeline.ack(:ack_ref, [], [failed_message])

      assert_receive {:requeued, [requeued_event]}
      assert requeued_event.retries == 3
    end

    test "drops messages that have exceeded max retries and deletes from queue", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 3)

      failed_message = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, %{source_id: source.id, backend_id: backend.id}},
        status: {:failed, "connection error"}
      }

      test_pid = self()

      Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, fn {sid, bid}, events ->
        send(test_pid, {:deleted_batch, sid, bid, events})
        :ok
      end)

      Mimic.reject(Logflare.Backends.IngestEventQueue, :add_to_table, 2)

      log =
        capture_log(fn ->
          Pipeline.ack(:ack_ref, [], [failed_message])
        end)

      assert log =~ "Dropping 1 ClickHouse events after 3 retries"
      assert_receive {:deleted_batch, _source_id, _backend_id, [deleted_event]}
      assert deleted_event.id == event.id
    end

    test "handles mixed retriable and exhausted messages", %{
      source: source,
      backend: backend
    } do
      retriable_event =
        build(:log_event, source: source, message: "Retriable") |> Map.put(:retries, 1)

      exhausted_event =
        build(:log_event, source: source, message: "Exhausted") |> Map.put(:retries, 3)

      failed_messages = [
        %Message{
          data: retriable_event,
          acknowledger: {Pipeline, :ack_id, %{source_id: source.id, backend_id: backend.id}},
          status: {:failed, "error"}
        },
        %Message{
          data: exhausted_event,
          acknowledger: {Pipeline, :ack_id, %{source_id: source.id, backend_id: backend.id}},
          status: {:failed, "error"}
        }
      ]

      test_pid = self()

      Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, 2, fn {_sid, _bid},
                                                                            events ->
        send(test_pid, {:deleted_batch, Enum.map(events, & &1.id)})
        :ok
      end)

      Mimic.expect(Logflare.Backends.IngestEventQueue, :add_to_table, fn {_sid, _bid}, events ->
        send(test_pid, {:requeued, events})
        :ok
      end)

      log =
        capture_log(fn ->
          Pipeline.ack(:ack_ref, [], failed_messages)
        end)

      # Both batches should be deleted (one for exhausted, one for retriable)
      assert_receive {:deleted_batch, [_exhausted_id]}
      assert_receive {:deleted_batch, [_retriable_id]}

      # Only the retriable event should be requeued
      assert_receive {:requeued, [requeued_event]}
      assert requeued_event.retries == 2
      assert requeued_event.body["event_message"] == "Retriable"
      assert log =~ "Dropping 1 ClickHouse events after 3 retries"
    end
  end

  describe "`handle_batch/4` failure handling" do
    test "marks all messages as failed when insert fails", %{
      context: context,
      source: source,
      backend: backend
    } do
      log_event = build(:log_event, source: source, message: "Test message")

      messages = [
        %Message{
          data: log_event,
          acknowledger: {Pipeline, :ack_id, %{source_id: source.id, backend_id: backend.id}}
        }
      ]

      Mimic.expect(ClickhouseAdaptor, :insert_log_events, fn {_source, _backend}, _events ->
        {:error, "Connection timeout"}
      end)

      result = Pipeline.handle_batch(:ch, messages, %{size: 1, trigger: :flush}, context)

      assert [%Message{status: {:failed, "Connection timeout"}}] = result
    end
  end
end
