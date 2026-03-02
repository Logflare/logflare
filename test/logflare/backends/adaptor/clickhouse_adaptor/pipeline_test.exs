defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline

  @one_hour_ns 3_600_000_000_000
  @trace_batch_info %Broadway.BatchInfo{batcher: :ch, batch_key: :trace, size: 1, trigger: :flush}
  @log_batch_info %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 1, trigger: :flush}

  setup do
    insert(:plan, name: "Free")

    {source, backend, cleanup_fn} = setup_clickhouse_test()
    on_exit(cleanup_fn)

    {:ok, supervisor_pid} = ClickHouseAdaptor.start_link(backend)

    on_exit(fn ->
      if Process.alive?(supervisor_pid) do
        Process.exit(supervisor_pid, :shutdown)
      end
    end)

    Process.sleep(200)

    context = %{backend_id: backend.id}

    [
      source: source,
      backend: backend,
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
    test "routes all messages to :ch batcher with `event_type` as batch_key", %{context: context} do
      log_event = build(:log_event)

      message = %Message{
        data: log_event,
        acknowledger: {Pipeline, :ack_id, :ack_data}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch, batch_key: :log} = result
      assert result.data == log_event
    end

    test "sets batch_key to `:metric` for metric events", %{context: context} do
      log_event = build(:log_event) |> Map.put(:event_type, :metric)

      message = %Message{
        data: log_event,
        acknowledger: {Pipeline, :ack_id, :ack_data}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch, batch_key: :metric} = result
    end

    test "sets batch_key to `:trace` for trace events", %{context: context} do
      log_event = build(:log_event) |> Map.put(:event_type, :trace)

      message = %Message{
        data: log_event,
        acknowledger: {Pipeline, :ack_id, :ack_data}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch, batch_key: :trace} = result
    end

    test "crashes when event_type is nil", %{context: context} do
      event = build(:log_event) |> Map.put(:event_type, nil)

      message = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, :ack_data}
      }

      assert_raise FunctionClauseError, fn ->
        Pipeline.handle_message(:default, message, context)
      end
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
        %Message{data: log_event1, acknowledger: {Pipeline, :ack_id, context}},
        %Message{data: log_event2, acknowledger: {Pipeline, :ack_id, context}}
      ]

      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 2, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      {:ok, query_result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      [first_row] = query_result
      assert %{"count" => 2} = first_row
    end

    test "handles empty messages list", %{context: context} do
      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 0, trigger: :flush}
      result = Pipeline.handle_batch(:ch, [], batch_info, context)
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
        %Message{data: log_event1, acknowledger: {Pipeline, :ack_id, context}},
        %Message{data: log_event2, acknowledger: {Pipeline, :ack_id, context}}
      ]

      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 2, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      {:ok, query_result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT event_message, timestamp FROM #{table_name} ORDER BY timestamp DESC"
        )

      assert length(query_result) == 2

      [first_row, second_row] = query_result

      assert first_row["event_message"] == "Another message"
      assert second_row["event_message"] == "Some message"

      assert first_row["timestamp"] != nil
      assert second_row["timestamp"] != nil
    end

    test "inserts events from multiple sources into single table", %{
      context: context,
      source: source,
      backend: backend
    } do
      user = insert(:user)
      source2 = insert(:source, user: user)

      event1 = build(:log_event, source: source, message: "Source 1 message")
      event2 = build(:log_event, source: source2, message: "Source 2 message")
      event3 = build(:log_event, source: source, message: "Source 1 message 2")

      messages = [
        %Message{data: event1, acknowledger: {Pipeline, :ack_id, context}},
        %Message{data: event2, acknowledger: {Pipeline, :ack_id, context}},
        %Message{data: event3, acknowledger: {Pipeline, :ack_id, context}}
      ]

      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 3, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      {:ok, query_result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      assert [%{"count" => 3}] = query_result
    end

    test "routes metric events to metrics table", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event, source: source, message: "Metric event")
        |> Map.put(:event_type, :metric)

      messages = [
        %Message{data: event, acknowledger: {Pipeline, :ack_id, context}}
      ]

      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :metric, size: 1, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :metric)

      {:ok, query_result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      assert [%{"count" => 1}] = query_result
    end

    test "routes trace events to traces table", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event, source: source, message: "Trace event") |> Map.put(:event_type, :trace)

      messages = [
        %Message{data: event, acknowledger: {Pipeline, :ack_id, context}}
      ]

      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :trace, size: 1, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      {:ok, query_result} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT count(*) as count FROM #{table_name}"
        )

      assert [%{"count" => 1}] = query_result
    end

    test "inserts logs with all scalar fields readable via SELECT", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event,
          source: source,
          message: "Full field test",
          metadata: %{"level" => "error", "region" => "us-east-1"}
        )

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 1, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      {:ok, [row]} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          """
          SELECT
            id, source_uuid, source_name, project, trace_id, span_id, trace_flags,
            severity_text, severity_number, service_name, event_message,
            scope_name, scope_version, scope_schema_url, resource_schema_url,
            resource_attributes, scope_attributes, log_attributes, timestamp
          FROM #{table_name}
          LIMIT 1
          """
        )

      assert row["id"] != nil
      assert is_binary(row["source_uuid"])
      assert row["event_message"] == "Full field test"
      assert row["severity_text"] == "ERROR"
      assert row["severity_number"] == 17
      assert is_binary(row["project"])
      assert is_binary(row["trace_id"])
      assert is_binary(row["span_id"])
      assert is_integer(row["trace_flags"])
      assert is_binary(row["service_name"])
      assert is_binary(row["scope_name"])
      assert is_binary(row["scope_version"])
      assert is_binary(row["scope_schema_url"])
      assert is_binary(row["resource_schema_url"])
      assert row["timestamp"] != nil
    end

    test "inserts metrics with all scalar fields readable via SELECT", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event,
          source: source,
          message: "Metric full field",
          metadata: %{
            "metric_name" => "http_requests",
            "metric_unit" => "1",
            "value" => 42.5
          }
        )
        |> Map.put(:event_type, :metric)

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :metric, size: 1, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :metric)

      {:ok, [row]} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          """
          SELECT
            id, source_uuid, source_name, project, time_unix, start_time_unix,
            metric_name, metric_description, metric_unit, metric_type,
            service_name, event_message, scope_name, scope_version,
            scope_schema_url, resource_schema_url,
            resource_attributes, scope_attributes, attributes,
            aggregation_temporality, is_monotonic, flags,
            value, count, sum, min, max,
            scale, zero_count, positive_offset, negative_offset,
            timestamp
          FROM #{table_name}
          LIMIT 1
          """
        )

      assert row["id"] != nil
      assert is_binary(row["source_uuid"])
      assert row["event_message"] == "Metric full field"
      assert is_binary(row["metric_name"])
      assert is_binary(row["metric_unit"])
      assert row["timestamp"] != nil
      assert row["time_unix"] != nil
    end

    test "inserts traces with all scalar fields readable via SELECT", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event,
          source: source,
          message: "Trace full field test"
        )
        |> Map.put(:event_type, :trace)

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :trace, size: 1, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      {:ok, [row]} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          """
          SELECT
            id, source_uuid, source_name, project, timestamp,
            trace_id, span_id, parent_span_id, trace_state,
            span_name, span_kind, service_name, event_message,
            duration, status_code, status_message,
            scope_name, scope_version,
            resource_attributes, span_attributes
          FROM #{table_name}
          LIMIT 1
          """
        )

      assert row["id"] != nil
      assert is_binary(row["source_uuid"])
      assert row["event_message"] == "Trace full field test"
      assert is_binary(row["trace_id"])
      assert is_binary(row["span_id"])
      assert is_binary(row["parent_span_id"])
      assert is_binary(row["span_name"])
      assert is_binary(row["span_kind"])
      assert is_binary(row["status_code"])
      assert is_binary(row["status_message"])
      assert is_integer(row["duration"])
      assert row["timestamp"] != nil
    end
  end

  describe "handle_batch/4 inferred timestamp replacement" do
    test "replaces timestamp with start_time for trace events when timestamp was inferred", %{
      context: context,
      source: source,
      backend: backend
    } do
      start_time_ns = System.system_time(:nanosecond) - @one_hour_ns

      event =
        build(:log_event,
          source: source,
          message: "Trace with inferred timestamp",
          start_time: start_time_ns,
          end_time: start_time_ns + 1_000_000
        )
        |> Map.put(:event_type, :trace)
        |> Map.put(:timestamp_inferred, true)

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      result = Pipeline.handle_batch(:ch, messages, @trace_batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      {:ok, [row]} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT toUnixTimestamp64Nano(timestamp) as ts_nano FROM #{table_name} LIMIT 1"
        )

      assert row["ts_nano"] == start_time_ns
    end

    test "preserves timestamp for trace events when timestamp was not inferred", %{
      context: context,
      source: source,
      backend: backend
    } do
      start_time_ns = System.system_time(:nanosecond) - @one_hour_ns

      event =
        build(:log_event,
          source: source,
          message: "Trace with explicit timestamp",
          start_time: start_time_ns,
          end_time: start_time_ns + 1_000_000
        )
        |> Map.put(:event_type, :trace)
        |> Map.put(:timestamp_inferred, false)

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      result = Pipeline.handle_batch(:ch, messages, @trace_batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      {:ok, [row]} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT toUnixTimestamp64Nano(timestamp) as ts_nano FROM #{table_name} LIMIT 1"
        )

      refute row["ts_nano"] == start_time_ns
    end

    test "preserves timestamp for trace events when timestamp inferred but no start_time", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event,
          source: source,
          message: "Trace without start_time"
        )
        |> Map.put(:event_type, :trace)
        |> Map.put(:timestamp_inferred, true)

      # Event body timestamp is in microseconds; mapper converts to nanoseconds
      expected_ts_nano = event.body["timestamp"] * 1_000

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      result = Pipeline.handle_batch(:ch, messages, @trace_batch_info, context)

      assert result == messages

      Process.sleep(200)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      {:ok, [row]} =
        ClickHouseAdaptor.execute_ch_query(
          backend,
          "SELECT toUnixTimestamp64Nano(timestamp) as ts_nano FROM #{table_name} LIMIT 1"
        )

      assert row["ts_nano"] == expected_ts_nano
    end

    test "does not replace timestamp for non-trace events even when timestamp was inferred", %{
      context: context,
      source: source
    } do
      start_time_ns = System.system_time(:nanosecond) - @one_hour_ns

      event =
        build(:log_event,
          source: source,
          message: "Log with inferred timestamp",
          start_time: start_time_ns
        )
        |> Map.put(:timestamp_inferred, true)

      messages = [%Message{data: event, acknowledger: {Pipeline, :ack_id, context}}]
      result = Pipeline.handle_batch(:ch, messages, @log_batch_info, context)

      assert result == messages
    end
  end

  describe "transform/2" do
    test "transforms event into Broadway message with correct acknowledger", %{backend: backend} do
      event = build(:log_event, message: "Test message")
      opts = [backend_id: backend.id]

      result = Pipeline.transform(event, opts)

      assert %Message{
               data: ^event,
               acknowledger: {Pipeline, :ack_id, %{backend_id: backend_id}}
             } = result

      assert is_integer(backend_id)
      assert backend_id == backend.id
    end
  end

  describe "ack/3" do
    test "returns :ok when failed list is empty" do
      assert Pipeline.ack(:ack_ref, [], []) == :ok
    end

    test "drops messages that have exceeded max retries and deletes from queue", %{
      source: source,
      backend: backend
    } do
      max_retries = Pipeline.max_retries()

      event =
        build(:log_event, source: source, message: "Test") |> Map.put(:retries, max_retries)

      failed_message = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
        status: {:failed, "connection error"}
      }

      test_pid = self()

      Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, fn {:consolidated, bid},
                                                                         events ->
        send(test_pid, {:deleted_batch, bid, events})
        :ok
      end)

      Mimic.reject(Logflare.Backends.IngestEventQueue, :add_to_table, 2)

      log =
        capture_log(fn ->
          Pipeline.ack(:ack_ref, [], [failed_message])
        end)

      assert log =~ "Dropping 1 ClickHouse events after #{max_retries} retries"
      assert_receive {:deleted_batch, _backend_id, [deleted_event]}
      assert deleted_event.id == event.id
    end
  end

  if Pipeline.max_retries() > 0 do
    describe "ack/3 retry behavior" do
      test "re-queues failed messages when `LogEvent` retries are under limit", %{
        source: source,
        backend: backend
      } do
        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)

        failed_message = %Message{
          data: event,
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
          status: {:failed, "connection error"}
        }

        test_pid = self()
        expected_backend_id = backend.id

        Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, fn {:consolidated, bid},
                                                                           events ->
          send(test_pid, {:deleted_batch, bid, events})
          :ok
        end)

        Mimic.expect(Logflare.Backends.IngestEventQueue, :add_to_table, fn {:consolidated, bid},
                                                                           events ->
          send(test_pid, {:requeued, bid, events})
          :ok
        end)

        Pipeline.ack(:ack_ref, [], [failed_message])

        assert_receive {:deleted_batch, ^expected_backend_id, [_deleted_event]}
        assert_receive {:requeued, ^expected_backend_id, [requeued_event]}
        assert requeued_event.retries == 1
      end

      test "increments retry count on each re-queue", %{
        source: source,
        backend: backend
      } do
        max_retries = Pipeline.max_retries()
        initial_retries = max_retries - 1

        event =
          build(:log_event, source: source, message: "Test") |> Map.put(:retries, initial_retries)

        failed_message = %Message{
          data: event,
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
          status: {:failed, "connection error"}
        }

        test_pid = self()

        Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, fn {:consolidated, _bid},
                                                                           _events ->
          :ok
        end)

        Mimic.expect(Logflare.Backends.IngestEventQueue, :add_to_table, fn {:consolidated, _bid},
                                                                           events ->
          send(test_pid, {:requeued, events})
          :ok
        end)

        Pipeline.ack(:ack_ref, [], [failed_message])

        assert_receive {:requeued, [requeued_event]}
        assert requeued_event.retries == initial_retries + 1
      end

      test "handles mixed retriable and exhausted messages", %{
        source: source,
        backend: backend
      } do
        max_retries = Pipeline.max_retries()

        retriable_event =
          build(:log_event, source: source, message: "Retriable") |> Map.put(:retries, 0)

        exhausted_event =
          build(:log_event, source: source, message: "Exhausted")
          |> Map.put(:retries, max_retries)

        failed_messages = [
          %Message{
            data: retriable_event,
            acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
            status: {:failed, "error"}
          },
          %Message{
            data: exhausted_event,
            acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
            status: {:failed, "error"}
          }
        ]

        test_pid = self()

        Mimic.expect(Logflare.Backends.IngestEventQueue, :delete_batch, 2, fn {:consolidated,
                                                                               _bid},
                                                                              events ->
          send(test_pid, {:deleted_batch, Enum.map(events, & &1.id)})
          :ok
        end)

        Mimic.expect(Logflare.Backends.IngestEventQueue, :add_to_table, fn {:consolidated, _bid},
                                                                           events ->
          send(test_pid, {:requeued, events})
          :ok
        end)

        log =
          capture_log(fn ->
            Pipeline.ack(:ack_ref, [], failed_messages)
          end)

        assert_receive {:deleted_batch, [_exhausted_id]}
        assert_receive {:deleted_batch, [_retriable_id]}

        assert_receive {:requeued, [requeued_event]}
        assert requeued_event.retries == 1
        assert requeued_event.body["event_message"] == "Retriable"
        assert log =~ "Dropping 1 ClickHouse events after #{max_retries} retries"
      end
    end
  end

  describe "handle_batch/4 failure handling" do
    test "marks all messages as failed when insert fails", %{
      context: context,
      source: source,
      backend: backend
    } do
      log_event = build(:log_event, source: source, message: "Test message")

      messages = [
        %Message{
          data: log_event,
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
        }
      ]

      Mimic.expect(ClickHouseAdaptor, :insert_log_events, fn _backend, _events, _event_type ->
        {:error, "Connection timeout"}
      end)

      batch_info = %Broadway.BatchInfo{batcher: :ch, batch_key: :log, size: 1, trigger: :flush}
      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert [%Message{status: {:failed, "Connection timeout"}}] = result
    end
  end
end
