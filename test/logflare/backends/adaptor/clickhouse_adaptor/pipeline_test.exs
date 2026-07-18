defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreaker
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer

  # Arbitrary day bucket value — pipeline only passes it through telemetry/OTEL
  # attributes, so these tests assert tuple shape, not the value itself.
  @day_bucket 20_594

  setup do
    insert(:plan, name: "Free")

    {source, backend} = setup_clickhouse_test()

    {:ok, supervisor_pid} = ClickHouseAdaptor.start_link(backend)

    on_exit(fn ->
      if Process.alive?(supervisor_pid) do
        Process.exit(supervisor_pid, :shutdown)
      end
    end)

    TestUtils.retry_assert(fn ->
      assert :ok = ClickHouseAdaptor.provision_ingest_tables(backend)
    end)

    context = %{backend_id: backend.id}

    [
      source: source,
      backend: backend,
      context: context
    ]
  end

  # Creates a temporary ETS table playing the role of a generation store, with events
  # inserted as {id, event} rows (see IngestEventQueue.lookup_event/2). Returns the tid.
  # Callers should NOT clean up the table — it is owned by the test process and
  # reclaimed when the test exits.
  defp setup_generation_events(events) do
    tid = :ets.new(:test_pipeline_generation, [:set, :public])
    for event <- events, do: :ets.insert(tid, {event.id, event})
    tid
  end

  # A real requeue (via IngestEventQueue.add_to_table/2) inserts the retried event under
  # a freshly generated gen_event_id, not the event's own id — so a post-requeue
  # generation table can't be looked up by event id directly. Scans by the event's own
  # id in the stored value instead of assuming it's the table's key.
  defp lookup_by_event_id(tid, event_id) do
    tid
    |> :ets.tab2list()
    |> Enum.find_value(fn {_gen_event_id, event} -> if event.id == event_id, do: event end)
  end

  # Builds a LogEventPointer for `event`, resolvable via `gen_tid` (see
  # setup_generation_events/1). `queue_tid` defaults to a fresh, otherwise-unused table
  # since most tests only care about claim/retry behavior driven off other fields.
  defp pointer_for(event, gen_tid, queue_tid \\ nil) do
    %LogEventPointer{
      id: event.id,
      tid: gen_tid,
      gen_event_id: event.id,
      queue_tid: queue_tid || :ets.new(:test_pipeline_queue, [:set, :public]),
      size: :erlang.external_size(event.body),
      retries: event.retries || 0,
      event_type: event.event_type,
      day_bucket: event.day_bucket,
      ingest_freshness: event.ingest_freshness
    }
  end

  # Builds a message in the format produced by handle_message/3, for use in
  # handle_batch/4 and ack/3 tests.
  defp batch_message(event, gen_tid, backend_id, queue_tid \\ nil) do
    %Message{
      data: pointer_for(event, gen_tid, queue_tid),
      acknowledger: {Pipeline, :ack_id, %{backend_id: backend_id}}
    }
  end

  # handle_batch/4 is not required to preserve input order (Broadway partitions and
  # re-reverses its return by status internally), so compare message sets by id
  # rather than list order.
  defp assert_same_messages(result, expected) do
    sort_key = fn %{data: %LogEventPointer{id: id}} -> id end
    assert Enum.sort_by(result, sort_key) == Enum.sort_by(expected, sort_key)
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

  describe "transform/2" do
    test "wraps the pointer unchanged as message data, with correct acknowledger", %{
      backend: backend
    } do
      event = build(:log_event, message: "Test message")
      gen_tid = setup_generation_events([event])
      pointer = pointer_for(event, gen_tid)
      opts = [backend_id: backend.id]

      result = Pipeline.transform(pointer, opts)

      assert %Message{
               data: ^pointer,
               acknowledger: {Pipeline, :ack_id, %{backend_id: backend_id}}
             } = result

      assert backend_id == backend.id
    end
  end

  describe "handle_message/3" do
    test "routes fresh log events to :ch_fresh batcher keyed by {event_type, day_bucket}", %{
      context: context,
      backend: backend
    } do
      event = build(:log_event)
      gen_tid = setup_generation_events([event])
      pointer = pointer_for(event, gen_tid)

      message = %Message{
        data: pointer,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch_fresh, batch_key: {:log, day_bucket}} = result
      assert result.data == pointer
      assert day_bucket == event.day_bucket
    end

    test "keys metric events by `{:metric, day_bucket}`", %{context: context, backend: backend} do
      event = build(:log_event) |> Map.put(:event_type, :metric)
      gen_tid = setup_generation_events([event])
      pointer = pointer_for(event, gen_tid)

      message = %Message{
        data: pointer,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch_fresh, batch_key: {:metric, _}} = result
    end

    test "keys trace events by `{:trace, day_bucket}`", %{context: context, backend: backend} do
      event = build(:log_event) |> Map.put(:event_type, :trace)
      gen_tid = setup_generation_events([event])
      pointer = pointer_for(event, gen_tid)

      message = %Message{
        data: pointer,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch_fresh, batch_key: {:trace, _}} = result
    end

    test "routes stale events to :ch_stale batcher", %{context: context, backend: backend} do
      event = build(:log_event) |> Map.put(:ingest_freshness, :stale)
      gen_tid = setup_generation_events([event])
      pointer = pointer_for(event, gen_tid)

      message = %Message{
        data: pointer,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{batcher: :ch_stale, batch_key: {:log, _}} = result
    end

    test "fails message when event_type is nil", %{context: context, backend: backend} do
      event = build(:log_event) |> Map.put(:event_type, nil)
      gen_tid = setup_generation_events([event])
      pointer = pointer_for(event, gen_tid)

      message = %Message{
        data: pointer,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
      }

      result = Pipeline.handle_message(:default, message, context)

      assert %Message{status: {:failed, :not_found}} = result
    end
  end

  describe "handle_batch/4" do
    test "extracts events from the generation store and inserts into ClickHouse", %{
      context: context,
      source: source,
      backend: backend
    } do
      log_event1 = build(:log_event, source: source, message: "Test message 1")
      log_event2 = build(:log_event, source: source, message: "Test message 2")
      gen_tid = setup_generation_events([log_event1, log_event2])

      messages = [
        batch_message(log_event1, gen_tid, backend.id),
        batch_message(log_event2, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[%{"count" => 2}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT count(*) as count FROM #{table_name}"
                 )
      end)
    end

    test "handles empty messages list", %{context: context} do
      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 0,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, [], batch_info, context)
      assert result == []
    end

    test "does not insert to ClickHouse when every routed row is missing from the generation store",
         %{
           context: context,
           source: source,
           backend: backend
         } do
      event1 = build(:log_event, source: source, message: "gone 1")
      event2 = build(:log_event, source: source, message: "gone 2")

      # Empty generation table: rows were claimed via pop_pending_pointers/2 but their
      # generation rotated out (or was otherwise dropped) before batch time, so
      # encode_message finds nothing.
      gen_tid = setup_generation_events([])

      messages = [
        batch_message(event1, gen_tid, backend.id),
        batch_message(event2, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :flush
      }

      Mimic.reject(ClickHouseAdaptor, :insert_log_events_compressed, 4)

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert length(result) == 2
      assert Enum.all?(result, &match?(%Message{status: {:failed, :not_found}}, &1))
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

      gen_tid = setup_generation_events([log_event1, log_event2])

      messages = [
        batch_message(log_event1, gen_tid, backend.id),
        batch_message(log_event2, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      query_result =
        TestUtils.retry_assert(fn ->
          assert {:ok, {query_result, _bytes}} =
                   ClickHouseAdaptor.execute_ch_query(
                     backend,
                     "SELECT event_message, timestamp FROM #{table_name} ORDER BY timestamp DESC"
                   )

          assert length(query_result) == 2
          query_result
        end)

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
      gen_tid = setup_generation_events([event1, event2, event3])

      messages = [
        batch_message(event1, gen_tid, backend.id),
        batch_message(event2, gen_tid, backend.id),
        batch_message(event3, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 3,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[%{"count" => 3}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT count(*) as count FROM #{table_name}"
                 )
      end)
    end

    test "routes metric events to metrics table", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event, source: source, message: "Metric event")
        |> Map.put(:event_type, :metric)

      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:metric, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :metric)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[%{"count" => 1}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT count(*) as count FROM #{table_name}"
                 )
      end)
    end

    test "routes trace events to traces table", %{
      context: context,
      source: source,
      backend: backend
    } do
      event =
        build(:log_event, source: source, message: "Trace event") |> Map.put(:event_type, :trace)

      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:trace, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[%{"count" => 1}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT count(*) as count FROM #{table_name}"
                 )
      end)
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

      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      row =
        TestUtils.retry_assert(fn ->
          assert {:ok, {[row], _bytes}} =
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

          row
        end)

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

      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:metric, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :metric)

      row =
        TestUtils.retry_assert(fn ->
          assert {:ok, {[row], _bytes}} =
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

          row
        end)

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

      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:trace, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      row =
        TestUtils.retry_assert(fn ->
          assert {:ok, {[row], _bytes}} =
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

          row
        end)

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

  describe "ack/3" do
    test "returns :ok when both lists are empty" do
      assert Pipeline.ack(:ack_ref, [], []) == :ok
    end

    test "deletes the event from the generation store for successful messages, without recording it into the recent-events cache",
         %{
           backend: backend
         } do
      event = build(:log_event, message: "Test")
      gen_tid = setup_generation_events([event])
      message = batch_message(event, gen_tid, backend.id)

      assert :ok = Pipeline.ack(:ack_ref, [message], [])

      # ack actively deletes the event row — it does not wait for GenerationJanitor's
      # rotation, which is only a failsafe for abandoned claims
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

      # never recorded: list_recent_logs_local/2 short-circuits to [] for any
      # consolidated backend without ever reading this cache, so writing here would
      # just be an unbounded, unread cost per event
      assert IngestEventQueue.list_recent_events({:consolidated, backend.id}, 10) == []
    end

    test "drops messages that have exceeded max retries, deleting them from the generation store",
         %{
           source: source,
           backend: backend
         } do
      max_retries = Pipeline.max_retries()
      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, max_retries)
      gen_tid = setup_generation_events([event])

      failed_message = %Message{
        data: pointer_for(event, gen_tid),
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
        status: {:failed, "connection error"}
      }

      log =
        capture_log(fn ->
          Pipeline.ack(:ack_ref, [], [failed_message])
        end)

      assert log =~ "Dropping 1 ClickHouse events: exhausted #{max_retries} retries"
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil
    end
  end

  if Pipeline.max_retries() > 0 do
    describe "ack/3 retry behavior" do
      test "re-queues failed messages when the pointer's retries are under limit", %{
        source: source,
        backend: backend
      } do
        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        gen_tid = setup_generation_events([event])

        failed_message = %Message{
          data: pointer_for(event, gen_tid),
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
          status: {:failed, "connection error"}
        }

        Pipeline.ack(:ack_ref, [], [failed_message])

        # old copy deleted from the generation it failed in ...
        assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

        # ... and re-added to the current generation with incremented retries
        current_gen_tid = IngestEventQueue.current_generation_tid({:consolidated, backend.id})
        assert %{retries: 1} = lookup_by_event_id(current_gen_tid, event.id)
      end

      test "increments retry count on each re-queue", %{
        source: source,
        backend: backend
      } do
        max_retries = Pipeline.max_retries()
        initial_retries = max_retries - 1

        event =
          build(:log_event, source: source, message: "Test") |> Map.put(:retries, initial_retries)

        gen_tid = setup_generation_events([event])

        failed_message = %Message{
          data: pointer_for(event, gen_tid),
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
          status: {:failed, "connection error"}
        }

        Pipeline.ack(:ack_ref, [], [failed_message])

        current_gen_tid = IngestEventQueue.current_generation_tid({:consolidated, backend.id})

        assert %{retries: ^initial_retries} = event
        assert %{retries: retries} = lookup_by_event_id(current_gen_tid, event.id)
        assert retries == initial_retries + 1
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

        gen_tid = setup_generation_events([retriable_event, exhausted_event])

        failed_messages = [
          %Message{
            data: pointer_for(retriable_event, gen_tid),
            acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
            status: {:failed, "error"}
          },
          %Message{
            data: pointer_for(exhausted_event, gen_tid),
            acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
            status: {:failed, "error"}
          }
        ]

        log =
          capture_log(fn ->
            Pipeline.ack(:ack_ref, [], failed_messages)
          end)

        assert log =~ "Dropping 1 ClickHouse events: exhausted #{max_retries} retries"

        # exhausted event's event row is deleted from the generation store, never re-added
        assert IngestEventQueue.lookup_event(gen_tid, exhausted_event.id) == nil

        current_gen_tid = IngestEventQueue.current_generation_tid({:consolidated, backend.id})
        assert lookup_by_event_id(current_gen_tid, exhausted_event.id) == nil

        # retriable event's old copy is deleted too, but re-added to the current
        # generation with incremented retries
        assert IngestEventQueue.lookup_event(gen_tid, retriable_event.id) == nil
        assert %{retries: 1} = lookup_by_event_id(current_gen_tid, retriable_event.id)
      end

      test "emits telemetry and logs a warning when a retriable event's generation is already gone",
           %{source: source, backend: backend} do
        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        gen_tid = setup_generation_events([event])

        # simulate GenerationJanitor dropping the generation before the retry's own
        # lookup — same race covered for pop_pending/2, but here on the requeue path
        :ets.delete(gen_tid)

        failed_message = %Message{
          data: pointer_for(event, gen_tid),
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
          status: {:failed, "connection error"}
        }

        telemetry_event = [:logflare, :ingest_event_queue, :requeue_lookup_miss]
        ref = :telemetry_test.attach_event_handlers(self(), [telemetry_event])
        on_exit(fn -> :telemetry.detach(ref) end)

        log = capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed_message]) end)

        assert log =~ "Dropped 1 ClickHouse event(s) during retry requeue"
        assert_receive {^telemetry_event, ^ref, %{count: 1}, %{backend_id: backend_id}}
        assert backend_id == backend.id

        # nothing landed in the current generation — the event was already gone at
        # lookup time
        current_gen_tid = IngestEventQueue.current_generation_tid({:consolidated, backend.id})
        assert lookup_by_event_id(current_gen_tid, event.id) == nil
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
      gen_tid = setup_generation_events([log_event])
      messages = [batch_message(log_event, gen_tid, backend.id)]

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        _opts ->
        {:error, "Connection timeout"}
      end)

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert [%Message{status: {:failed, "Connection timeout"}}] = result
    end
  end

  describe "handle_batch/4 async routing" do
    setup %{source: source, backend: backend} do
      log_event = build(:log_event, source: source, message: "Test message")
      gen_tid = setup_generation_events([log_event])
      messages = [batch_message(log_event, gen_tid, backend.id)]
      [messages: messages]
    end

    test "routes stale batches through async inserts", %{
      context: context,
      messages: messages
    } do
      test_pid = self()

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        opts ->
        send(test_pid, {:insert_opts, opts})
        :ok
      end)

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_stale,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :timeout
      }

      Pipeline.handle_batch(:ch_stale, messages, batch_info, context)

      assert_receive {:insert_opts, opts}
      assert Keyword.get(opts, :async) == true
    end

    test "routes fresh batches through synchronous inserts", %{
      context: context,
      messages: messages
    } do
      test_pid = self()

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        opts ->
        send(test_pid, {:insert_opts, opts})
        :ok
      end)

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_receive {:insert_opts, opts}
      assert Keyword.get(opts, :async) == false
    end
  end

  describe "handle_batch/4 circuit breaker" do
    test "does not consult the breaker before inserting (initial attempts are never gated)", %{
      context: context,
      source: source,
      backend: backend
    } do
      test_pid = self()

      Mimic.reject(CircuitBreaker, :check, 1)

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        _opts ->
        send(test_pid, :inserted)
        :ok
      end)

      log_event = build(:log_event, source: source, message: "Test message")
      gen_tid = setup_generation_events([log_event])
      messages = [batch_message(log_event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      assert [%Message{status: :ok}] =
               Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_received :inserted
    end

    test "records a failure when the insert fails", %{
      context: context,
      source: source,
      backend: backend
    } do
      test_pid = self()
      expected_backend_id = backend.id

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        _opts ->
        {:error, "boom"}
      end)

      Mimic.expect(CircuitBreaker, :record_failure, fn %{id: id} ->
        send(test_pid, {:recorded_failure, id})
        :ok
      end)

      log_event = build(:log_event, source: source, message: "Test message")
      gen_tid = setup_generation_events([log_event])
      messages = [batch_message(log_event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch_fresh,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      assert [%Message{status: {:failed, "boom"}}] =
               Pipeline.handle_batch(:ch_fresh, messages, batch_info, context)

      assert_received {:recorded_failure, ^expected_backend_id}
    end
  end

  describe "ack/3 circuit breaker" do
    test "sheds retriable messages instead of requeuing when the breaker is open", %{
      source: source,
      backend: backend
    } do
      Mimic.stub(CircuitBreaker, :check, fn _backend_id -> {:error, :circuit_open, 0} end)
      Mimic.reject(IngestEventQueue, :add_to_table, 2)

      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
      gen_tid = setup_generation_events([event])

      failed_message = %Message{
        data: pointer_for(event, gen_tid),
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
        status: {:failed, "boom"}
      }

      log = capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed_message]) end)

      assert log =~ "circuit breaker open"
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil
    end

    test "requeues retriable messages when the breaker is closed", %{
      source: source,
      backend: backend
    } do
      Mimic.stub(CircuitBreaker, :check, fn _backend_id -> :ok end)

      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
      gen_tid = setup_generation_events([event])

      failed_message = %Message{
        data: pointer_for(event, gen_tid),
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
        status: {:failed, "boom"}
      }

      Pipeline.ack(:ack_ref, [], [failed_message])

      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

      current_gen_tid = IngestEventQueue.current_generation_tid({:consolidated, backend.id})
      assert %{retries: 1} = lookup_by_event_id(current_gen_tid, event.id)
    end
  end
end
