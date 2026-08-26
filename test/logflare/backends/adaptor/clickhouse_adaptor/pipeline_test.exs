defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.CircuitBreaker
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodedRow
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline
  alias Logflare.Backends.DynamicPipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer
  alias Logflare.Mapper
  alias Logflare.Mapper.OtelDefaults
  alias Logflare.TestUtils

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

    context = Pipeline.build_processor_context(backend.id)

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
    |> Enum.find_value(fn
      {_gen_event_id, %EncodedRow{pointer: %{id: ^event_id}} = encoded} -> encoded
      {_gen_event_id, %{id: ^event_id} = event} -> event
      _row -> nil
    end)
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
      day_bucket: event.day_bucket
    }
  end

  defp queued_pointer(queue_tid, event_id) do
    case :ets.lookup(queue_tid, event_id) do
      [{^event_id, gen_tid, gen_event_id, size, retries, event_type, day_bucket}] ->
        %LogEventPointer{
          id: event_id,
          tid: gen_tid,
          gen_event_id: gen_event_id,
          queue_tid: queue_tid,
          size: size,
          retries: retries,
          event_type: event_type,
          day_bucket: day_bucket
        }

      [] ->
        nil
    end
  end

  # Builds a message in the format produced by handle_message/3, for use in
  # handle_batch/4 and ack/3 tests.
  defp batch_message(event, gen_tid, backend_id, queue_tid \\ nil, in_flight_ref \\ nil) do
    message = %Message{
      data: pointer_for(event, gen_tid, queue_tid),
      acknowledger: {Pipeline, :ack_id, %{backend_id: backend_id, in_flight_ref: in_flight_ref}}
    }

    Pipeline.handle_message(:default, message, Pipeline.build_processor_context(backend_id))
  end

  # handle_batch/4 is not required to preserve input order (Broadway partitions and
  # re-reverses its return by status internally), so compare message sets by id
  # rather than list order.
  defp assert_same_messages(result, expected) do
    assert Enum.sort_by(result, &message_id/1) == Enum.sort_by(expected, &message_id/1)
  end

  defp message_id(%Message{data: %EncodedRow{pointer: %{id: id}}}), do: id
  defp message_id(%Message{data: %LogEventPointer{id: id}}), do: id

  describe "child_spec/1" do
    test "returns proper child specification" do
      spec = Pipeline.child_spec(:some_arg)

      assert spec.id == Pipeline
      assert spec.start == {Pipeline, :start_link, [:some_arg]}
    end

    test "scales processors with schedulers while reserving four batch processors" do
      assert Pipeline.processor_concurrency(1) == 6
      assert Pipeline.processor_concurrency(6) == 6
      assert Pipeline.processor_concurrency(12) == 8
      assert Pipeline.processor_concurrency(32) == 28
    end

    test "starts Broadway with runtime-derived processor concurrency", %{backend: backend} do
      dynamic_pipeline_name = Backends.via_backend(backend, Pipeline)
      assert [pipeline_name] = DynamicPipeline.list_pipelines(dynamic_pipeline_name)

      topology = Broadway.topology(pipeline_name)
      assert [processor] = topology[:processors]
      assert [batcher] = topology[:batchers]

      assert processor.concurrency == Pipeline.processor_concurrency()
      assert batcher.concurrency == 4
    end

    test "retains 64 batches of in-flight capacity independently of insert concurrency" do
      assert Pipeline.max_in_flight() == 64 * Pipeline.max_batch_size()
      assert Pipeline.max_in_flight() == 3_840_000
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
    test "routes log events to :ch batcher keyed by {event_type, day_bucket}", %{
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

      assert %Message{
               batcher: :ch,
               batch_key: {:log, day_bucket},
               data: %EncodedRow{pointer: ^pointer, row: row}
             } = result

      assert is_binary(row)

      assert %EncodedRow{pointer: ^pointer, row: ^row} =
               IngestEventQueue.lookup_event(gen_tid, event.id)

      assert day_bucket == event.day_bucket
    end

    test "keeps an encoded row when its generation disappears after lookup", %{
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

      Mimic.expect(IngestEventQueue, :replace_event, fn ^gen_tid, id, %EncodedRow{} ->
        assert id == event.id
        :ets.delete(gen_tid)
        {:error, :not_found}
      end)

      assert %Message{
               status: :ok,
               data: %EncodedRow{pointer: ^pointer, row: row}
             } = Pipeline.handle_message(:default, message, context)

      assert is_binary(row)
    end

    test "aggregates missing-id telemetry when processors cannot resolve events", %{
      context: context,
      backend: backend
    } do
      events = build_list(3, :log_event)
      gen_tid = setup_generation_events([])

      failed_messages =
        Enum.map(events, fn event ->
          message = %Message{
            data: pointer_for(event, gen_tid),
            acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
          }

          assert %Message{status: {:failed, :not_found}} =
                   Pipeline.handle_message(:default, message, context)
        end)

      telemetry_event = [:logflare, :ingest_event_queue, :missing_ids]
      ref = :telemetry_test.attach_event_handlers(self(), [telemetry_event])
      on_exit(fn -> :telemetry.detach(ref) end)

      capture_log(fn -> assert :ok = Pipeline.ack(:ack_ref, [], failed_messages) end)

      assert_receive {^telemetry_event, ^ref, %{count: 3}, metadata}
      assert metadata.backend_type == :clickhouse
      assert metadata.backend_id == backend.id
      assert metadata.event_type == :log
      refute_receive {^telemetry_event, ^ref, _, _}
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

      assert %Message{batcher: :ch, batch_key: {:metric, _}} = result
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

      assert %Message{batcher: :ch, batch_key: {:trace, _}} = result
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
    test "compresses processor-encoded rows and inserts into ClickHouse", %{
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
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :size
      }

      telemetry_event = [:logflare, :backends, :pipeline, :handle_batch]
      TestUtils.attach_forwarder(telemetry_event)

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert_same_messages(result, messages)

      assert_receive {:telemetry_event, ^telemetry_event, %{batch_size: 2, batch_trigger: :size},
                      %{
                        backend_type: :clickhouse,
                        backend_id: backend_id,
                        event_type: :log,
                        batch_trigger: :size,
                        day_bucket: @day_bucket
                      }}

      assert backend_id == backend.id

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[%{"count" => 2}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT count(*) as count FROM #{table_name}"
                 )
      end)
    end

    test "compresses encoded rows while preserving missing messages", %{
      context: context,
      source: source,
      backend: backend
    } do
      events =
        Enum.map(1..10, fn index ->
          build(:log_event, source: source, message: "Sequential event #{index}")
        end)

      stored_events = List.delete_at(events, 4)
      gen_tid = setup_generation_events(stored_events)
      messages = Enum.map(events, &batch_message(&1, gen_tid, backend.id))

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 10,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert Enum.count(result, &match?(%Message{status: :ok}, &1)) == 9
      assert Enum.count(result, &match?(%Message{status: {:failed, :not_found}}, &1)) == 1

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)
      expected_messages = stored_events |> Enum.map(& &1.body["event_message"]) |> Enum.sort()

      TestUtils.retry_assert(fn ->
        assert {:ok, {rows, _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT event_message FROM #{table_name}"
                 )

        assert rows |> Enum.map(& &1["event_message"]) |> Enum.sort() == expected_messages
      end)
    end

    test "isolates mapping failures while inserting valid rows", %{
      context: context,
      source: source,
      backend: backend
    } do
      valid_event = build(:log_event, source: source, message: "valid row")

      invalid_event =
        build(:log_event, source: source, message: "invalid row")
        |> Map.put(:id, "not-a-uuid")

      missing_event = build(:log_event, source: source, message: "missing row")
      gen_tid = setup_generation_events([valid_event, invalid_event])

      messages = [
        batch_message(valid_event, gen_tid, backend.id),
        batch_message(invalid_event, gen_tid, backend.id),
        batch_message(missing_event, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 3,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      statuses = Map.new(result, fn message -> {message_id(message), message.status} end)

      assert statuses[valid_event.id] == :ok
      assert statuses[invalid_event.id] == {:failed, "invalid event UUID"}
      assert statuses[missing_event.id] == {:failed, :not_found}

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[%{"event_message" => "valid row"}], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   "SELECT event_message FROM #{table_name}"
                 )
      end)
    end

    test "handles empty messages list", %{context: context} do
      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 0,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, [], batch_info, context)
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

      # Empty generation table: processors fail these messages before they can enter a
      # real batch. Passing them directly here also proves the batch callback skips an
      # empty RowBinary insert defensively.
      gen_tid = setup_generation_events([])

      messages = [
        batch_message(event1, gen_tid, backend.id),
        batch_message(event2, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :flush
      }

      Mimic.reject(ClickHouseAdaptor, :insert_log_events_compressed, 4)

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

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
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
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
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 3,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

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
        batcher: :ch,
        batch_key: {:metric, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

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
        batcher: :ch,
        batch_key: {:trace, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

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

    test "round-trips every log column with distinct values", %{
      context: context,
      source: source,
      backend: backend
    } do
      timestamp = System.system_time(:microsecond)

      event =
        build(:log_event,
          source: source,
          ingested_at: nil,
          project: "log-project",
          trace_id: "log-trace",
          span_id: "log-span",
          trace_flags: 7,
          severity_text: "warn",
          severity_number: 19,
          resource: %{
            "service" => %{"name" => "log-service"},
            "schema_url" => "log-resource-schema",
            "region" => "log-region"
          },
          scope: %{
            "name" => "log-scope",
            "version" => "log-v1",
            "schema_url" => "log-scope-schema",
            "attributes" => %{"scope-key" => "scope-value"}
          },
          event_message: "log-message",
          custom_log: "log-attribute",
          timestamp: timestamp
        )
        |> Map.put(:ingested_at, nil)

      map_compiled = Mapper.compile!(OtelDefaults.for_type(:log, :map))
      mapped_body = Mapper.map(event.body, map_compiled)
      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :log)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[row], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   """
                   SELECT
                     id, source_uuid, source_name, project, trace_id, span_id, trace_flags,
                     severity_text, severity_number, service_name, event_message,
                     scope_name, scope_version, scope_schema_url, resource_schema_url,
                     resource_attributes, scope_attributes, log_attributes,
                     mapping_config_id, ingested_at,
                     toUnixTimestamp64Nano(timestamp) AS timestamp_nano
                   FROM #{table_name}
                   """
                 )

        assert row["id"] == event.id
        assert row["source_uuid"] == Atom.to_string(event.source_uuid)
        assert row["source_name"] == event.source_name
        assert row["project"] == "log-project"
        assert row["trace_id"] == "log-trace"
        assert row["span_id"] == "log-span"
        assert row["trace_flags"] == 7
        assert row["severity_text"] == "WARN"
        assert row["severity_number"] == 19
        assert row["service_name"] == "log-service"
        assert row["event_message"] == "log-message"
        assert row["scope_name"] == "log-scope"
        assert row["scope_version"] == "log-v1"
        assert row["scope_schema_url"] == "log-scope-schema"
        assert row["resource_schema_url"] == "log-resource-schema"
        assert row["resource_attributes"] == mapped_body["resource_attributes"]
        assert row["scope_attributes"] == mapped_body["scope_attributes"]
        assert row["log_attributes"] == mapped_body["log_attributes"]
        assert row["mapping_config_id"] == OtelDefaults.config_id(:log)
        assert row["ingested_at"] == nil
        assert row["timestamp_nano"] == timestamp * 1_000
      end)
    end

    test "round-trips every metric column with distinct values", %{
      context: context,
      source: source,
      backend: backend
    } do
      timestamp = System.system_time(:microsecond)
      time_unix_nano = timestamp * 1_000 - 200
      start_time_unix_nano = timestamp * 1_000 - 500

      event =
        build(:log_event,
          source: source,
          project: "metric-project",
          time_unix_nano: time_unix_nano,
          start_time_unix_nano: start_time_unix_nano,
          metric_name: "metric-name",
          metric_description: "metric-description",
          metric_unit: "metric-unit",
          metric_type: "histogram",
          resource: %{
            "service" => %{"name" => "metric-service"},
            "schema_url" => "metric-resource-schema"
          },
          scope: %{
            "name" => "metric-scope",
            "version" => "metric-v1",
            "schema_url" => "metric-scope-schema",
            "attributes" => %{"scope-key" => "scope-value"}
          },
          event_message: "metric-message",
          custom_metric: "metric-attribute",
          aggregation_temporality: "delta",
          is_monotonic: true,
          flags: 123_456,
          value: 1.25,
          count: 2,
          sum: 3.5,
          min: -4.75,
          max: 5.875,
          scale: -6,
          zero_count: 7,
          positive_offset: 8,
          negative_offset: -9,
          bucket_counts: [10, 11],
          explicit_bounds: [12.5, 13.5],
          positive_bucket_counts: [14, 15],
          negative_bucket_counts: [16, 17],
          quantile_values: [18.5, 19.5],
          quantiles: [0.25, 0.75],
          exemplars: [
            %{
              "filtered_attributes" => %{"exemplar-key" => "exemplar-value"},
              "time_unix_nano" => time_unix_nano - 100,
              "value" => 20.5,
              "span_id" => "exemplar-span",
              "trace_id" => "exemplar-trace"
            }
          ],
          timestamp: timestamp
        )
        |> Map.put(:event_type, :metric)
        |> Map.put(:ingested_at, nil)

      map_compiled = Mapper.compile!(OtelDefaults.for_type(:metric, :map))
      mapped_body = Mapper.map(event.body, map_compiled)
      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:metric, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :metric)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[row], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   """
                   SELECT
                     id, source_uuid, source_name, project,
                     toUnixTimestamp64Nano(time_unix) AS time_unix_nano,
                     toUnixTimestamp64Nano(start_time_unix) AS start_time_unix_nano,
                     metric_name, metric_description, metric_unit,
                     toInt8(metric_type) AS metric_type,
                     service_name, event_message, scope_name, scope_version,
                     scope_schema_url, resource_schema_url,
                     resource_attributes, scope_attributes, attributes,
                     aggregation_temporality, toUInt8(is_monotonic) AS is_monotonic, flags,
                     value, count, sum, min, max, scale, zero_count,
                     positive_offset, negative_offset, bucket_counts, explicit_bounds,
                     positive_bucket_counts, negative_bucket_counts,
                     quantile_values, quantiles, `exemplars.filtered_attributes`,
                     arrayMap(value -> toUnixTimestamp64Nano(value), `exemplars.time_unix`)
                       AS exemplar_times,
                     `exemplars.value`, `exemplars.span_id`, `exemplars.trace_id`,
                     mapping_config_id, ingested_at,
                     toUnixTimestamp64Nano(timestamp) AS timestamp_nano
                   FROM #{table_name}
                   """
                 )

        assert row["id"] == event.id
        assert row["source_uuid"] == Atom.to_string(event.source_uuid)
        assert row["source_name"] == event.source_name
        assert row["project"] == "metric-project"
        assert row["time_unix_nano"] == mapped_body["time_unix"]
        assert row["start_time_unix_nano"] == mapped_body["start_time_unix"]
        assert row["metric_name"] == "metric-name"
        assert row["metric_description"] == "metric-description"
        assert row["metric_unit"] == "metric-unit"
        assert row["metric_type"] == mapped_body["metric_type"]
        assert row["service_name"] == "metric-service"
        assert row["event_message"] == "metric-message"
        assert row["scope_name"] == "metric-scope"
        assert row["scope_version"] == "metric-v1"
        assert row["scope_schema_url"] == "metric-scope-schema"
        assert row["resource_schema_url"] == "metric-resource-schema"
        assert row["resource_attributes"] == mapped_body["resource_attributes"]
        assert row["scope_attributes"] == mapped_body["scope_attributes"]
        assert row["attributes"] == mapped_body["attributes"]
        assert row["aggregation_temporality"] == "delta"
        assert row["is_monotonic"] == 1
        assert row["flags"] == 123_456
        assert row["value"] == 1.25
        assert row["count"] == 2
        assert row["sum"] == 3.5
        assert row["min"] == -4.75
        assert row["max"] == 5.875
        assert row["scale"] == -6
        assert row["zero_count"] == 7
        assert row["positive_offset"] == 8
        assert row["negative_offset"] == -9
        assert row["bucket_counts"] == mapped_body["bucket_counts"]
        assert row["explicit_bounds"] == mapped_body["explicit_bounds"]
        assert row["positive_bucket_counts"] == mapped_body["positive_bucket_counts"]
        assert row["negative_bucket_counts"] == mapped_body["negative_bucket_counts"]
        assert row["quantile_values"] == mapped_body["quantile_values"]
        assert row["quantiles"] == mapped_body["quantiles"]

        assert row["exemplars.filtered_attributes"] ==
                 mapped_body["exemplars.filtered_attributes"]

        assert row["exemplar_times"] == mapped_body["exemplars.time_unix"]
        assert row["exemplars.value"] == mapped_body["exemplars.value"]
        assert row["exemplars.span_id"] == mapped_body["exemplars.span_id"]
        assert row["exemplars.trace_id"] == mapped_body["exemplars.trace_id"]
        assert row["mapping_config_id"] == OtelDefaults.config_id(:metric)
        assert row["ingested_at"] == nil
        assert row["timestamp_nano"] == timestamp * 1_000
      end)
    end

    test "round-trips every trace column with distinct values", %{
      context: context,
      source: source,
      backend: backend
    } do
      timestamp = System.system_time(:microsecond)
      start_time = timestamp * 1_000 - 1_000
      end_time = start_time + 600

      event =
        build(:log_event,
          source: source,
          project: "trace-project",
          trace_id: "trace-id",
          span_id: "span-id",
          parent_span_id: "parent-span-id",
          trace_state: "vendor=value",
          span_name: "trace-span-name",
          span_kind: "server",
          resource: %{
            "service" => %{"name" => "trace-service"},
            "schema_url" => "trace-resource-schema"
          },
          event_message: "trace-message",
          duration: 0,
          start_time: start_time,
          end_time: end_time,
          status: %{"code" => "ERROR", "message" => "trace-status-message"},
          scope: %{"name" => "trace-scope", "version" => "trace-v1"},
          custom_span: "span-attribute",
          events: [
            %{
              "time_unix_nano" => start_time + 100,
              "name" => "trace-event",
              "attributes" => %{"event-key" => "event-value"}
            }
          ],
          links: [
            %{
              "trace_id" => "linked-trace",
              "span_id" => "linked-span",
              "trace_state" => "linked-vendor=value",
              "attributes" => %{"link-key" => "link-value"}
            }
          ],
          timestamp: timestamp
        )
        |> Map.put(:event_type, :trace)
        |> Map.put(:ingested_at, nil)

      map_compiled = Mapper.compile!(OtelDefaults.for_type(:trace, :map))
      mapped_body = Mapper.map(event.body, map_compiled)
      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:trace, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)
      assert_same_messages(result, messages)

      table_name = ClickHouseAdaptor.clickhouse_ingest_table_name(backend, :trace)

      TestUtils.retry_assert(fn ->
        assert {:ok, {[row], _bytes}} =
                 ClickHouseAdaptor.execute_ch_query(
                   backend,
                   """
                   SELECT
                     id, source_uuid, source_name, project, trace_id, span_id,
                     parent_span_id, trace_state, span_name, span_kind,
                     service_name, event_message, duration, status_code, status_message,
                     scope_name, scope_version, resource_attributes, span_attributes,
                     arrayMap(value -> toUnixTimestamp64Nano(value), `events.timestamp`)
                       AS event_times,
                     `events.name`, `events.attributes`, `links.trace_id`, `links.span_id`,
                     `links.trace_state`, `links.attributes`, mapping_config_id, ingested_at,
                     toUnixTimestamp64Nano(timestamp) AS timestamp_nano
                   FROM #{table_name}
                   """
                 )

        assert row["id"] == event.id
        assert row["source_uuid"] == Atom.to_string(event.source_uuid)
        assert row["source_name"] == event.source_name
        assert row["project"] == "trace-project"
        assert row["trace_id"] == "trace-id"
        assert row["span_id"] == "span-id"
        assert row["parent_span_id"] == "parent-span-id"
        assert row["trace_state"] == "vendor=value"
        assert row["span_name"] == "trace-span-name"
        assert row["span_kind"] == "Server"
        assert row["service_name"] == "trace-service"
        assert row["event_message"] == "trace-message"
        assert row["duration"] == end_time - start_time
        assert row["status_code"] == "ERROR"
        assert row["status_message"] == "trace-status-message"
        assert row["scope_name"] == "trace-scope"
        assert row["scope_version"] == "trace-v1"
        assert row["resource_attributes"] == mapped_body["resource_attributes"]
        assert row["span_attributes"] == mapped_body["span_attributes"]
        assert row["event_times"] == mapped_body["events.timestamp"]
        assert row["events.name"] == mapped_body["events.name"]
        assert row["events.attributes"] == mapped_body["events.attributes"]
        assert row["links.trace_id"] == mapped_body["links.trace_id"]
        assert row["links.span_id"] == mapped_body["links.span_id"]
        assert row["links.trace_state"] == mapped_body["links.trace_state"]
        assert row["links.attributes"] == mapped_body["links.attributes"]
        assert row["mapping_config_id"] == OtelDefaults.config_id(:trace)
        assert row["ingested_at"] == nil
        assert row["timestamp_nano"] == timestamp * 1_000
      end)
    end
  end

  describe "handle_batch/4 async insert decision" do
    setup do
      {async_source, async_backend} =
        setup_clickhouse_test(
          config: %{use_async_inserts_for_small_batches: true, async_insert_max_rows: 2}
        )

      [async_source: async_source, async_backend: async_backend]
    end

    test "sends async: true when the encoded row count is below the cutoff", %{
      async_source: source,
      async_backend: backend
    } do
      test_pid = self()

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        opts ->
        send(test_pid, {:insert_opts, opts})
        :ok
      end)

      event = build(:log_event, source: source, message: "small batch")
      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      Pipeline.handle_batch(:ch, messages, batch_info, %{backend_id: backend.id})

      assert_received {:insert_opts, opts}
      assert Keyword.get(opts, :async) == true
    end

    test "sends async: false when the encoded row count is at or above the cutoff", %{
      async_source: source,
      async_backend: backend
    } do
      test_pid = self()

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        opts ->
        send(test_pid, {:insert_opts, opts})
        :ok
      end)

      event1 = build(:log_event, source: source, message: "row 1")
      event2 = build(:log_event, source: source, message: "row 2")
      gen_tid = setup_generation_events([event1, event2])

      messages = [
        batch_message(event1, gen_tid, backend.id),
        batch_message(event2, gen_tid, backend.id)
      ]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 2,
        trigger: :flush
      }

      Pipeline.handle_batch(:ch, messages, batch_info, %{backend_id: backend.id})

      assert_received {:insert_opts, opts}
      assert Keyword.get(opts, :async) == false
    end

    test "sends async: false when the backend has the feature disabled", %{
      source: source,
      backend: backend
    } do
      test_pid = self()

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        opts ->
        send(test_pid, {:insert_opts, opts})
        :ok
      end)

      event = build(:log_event, source: source, message: "disabled")
      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      Pipeline.handle_batch(:ch, messages, batch_info, %{backend_id: backend.id})

      assert_received {:insert_opts, opts}
      assert Keyword.get(opts, :async) == false
    end
  end

  describe "ack/3" do
    test "returns :ok when both lists are empty" do
      assert Pipeline.ack(:ack_ref, [], []) == :ok
    end

    test "decrements each producer's atomics ref across successful and failed messages", %{
      source: source,
      backend: backend
    } do
      ref_a = :atomics.new(1, signed: true)
      ref_b = :atomics.new(1, signed: true)
      :atomics.put(ref_a, 1, 2)
      :atomics.put(ref_b, 1, 1)

      successful_event = build(:log_event, source: source)

      failed_event_a =
        build(:log_event, source: source) |> Map.put(:retries, Pipeline.max_retries())

      failed_event_b =
        build(:log_event, source: source) |> Map.put(:retries, Pipeline.max_retries())

      gen_tid = setup_generation_events([successful_event, failed_event_a, failed_event_b])

      successful = [batch_message(successful_event, gen_tid, backend.id, nil, ref_a)]

      failed = [
        batch_message(failed_event_a, gen_tid, backend.id, nil, ref_a)
        |> Message.failed(:insert_failed),
        batch_message(failed_event_b, gen_tid, backend.id, nil, ref_b)
        |> Message.failed(:insert_failed)
      ]

      capture_log(fn -> assert :ok = Pipeline.ack(:ack_ref, successful, failed) end)

      assert :atomics.get(ref_a, 1) == 0
      assert :atomics.get(ref_b, 1) == 0
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
      test "re-queues the encoded row without mapping it again", %{
        source: source,
        backend: backend
      } do
        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        gen_tid = setup_generation_events([event])
        queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])

        failed_message =
          event
          |> batch_message(gen_tid, backend.id, queue_tid)
          |> Message.failed("connection error")

        assert %EncodedRow{row: original_row} = failed_message.data

        telemetry_event = [:logflare, :ingest_event_queue, :requeue_deduplicated]
        ref = :telemetry_test.attach_event_handlers(self(), [telemetry_event])
        on_exit(fn -> :telemetry.detach(ref) end)

        Pipeline.ack(:ack_ref, [], [failed_message])

        assert %LogEventPointer{retries: 1} = retry_pointer = queued_pointer(queue_tid, event.id)
        refute retry_pointer.tid == gen_tid
        refute retry_pointer.gen_event_id == event.id
        assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

        assert %EncodedRow{pointer: ^retry_pointer, row: ^original_row} =
                 IngestEventQueue.lookup_event(retry_pointer.tid, retry_pointer.gen_event_id)

        # The retry no longer depends on the old generation remaining alive.
        :ets.delete(gen_tid)

        retry_message = %Message{
          data: retry_pointer,
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
        }

        assert %Message{data: %EncodedRow{row: ^original_row}} =
                 Pipeline.handle_message(
                   :default,
                   retry_message,
                   Pipeline.build_processor_context(backend.id)
                 )

        refute_receive {^telemetry_event, ^ref, _, _}
      end

      test "drops an encoded row when its published retry also fails", %{
        source: source,
        backend: backend,
        context: context
      } do
        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        gen_tid = setup_generation_events([event])
        retry_key = {:consolidated, backend.id, self()}
        assert {:ok, queue_tid} = IngestEventQueue.upsert_tid(retry_key)

        first_failure =
          event
          |> batch_message(gen_tid, backend.id, queue_tid)
          |> Message.failed("first connection error")

        assert %EncodedRow{row: original_row} = first_failure.data
        assert :ok = Pipeline.ack(:ack_ref, [], [first_failure])

        assert {:ok, [retry_pointer], ^queue_tid} =
                 IngestEventQueue.pop_pending_pointers(retry_key, 1)

        assert retry_pointer.retries == Pipeline.max_retries()

        retry_message = %Message{
          data: retry_pointer,
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}}
        }

        assert %Message{data: %EncodedRow{row: ^original_row}} =
                 processed_retry =
                 Pipeline.handle_message(:default, retry_message, context)

        log =
          capture_log(fn ->
            assert :ok =
                     Pipeline.ack(
                       :ack_ref,
                       [],
                       [Message.failed(processed_retry, "second connection error")]
                     )
          end)

        assert log =~ "Dropping 1 ClickHouse events: exhausted #{Pipeline.max_retries()} retries"
        assert IngestEventQueue.total_pending(retry_key) == 0
        assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

        assert IngestEventQueue.lookup_event(
                 retry_pointer.tid,
                 retry_pointer.gen_event_id
               ) == nil
      end

      test "reports and cleans up an encoded retry deduplicated by a newer pointer", %{
        source: source,
        backend: backend
      } do
        event = build(:log_event, source: source, message: "claimed") |> Map.put(:retries, 0)
        old_gen_tid = setup_generation_events([event])
        queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])

        failed_message =
          event
          |> batch_message(old_gen_tid, backend.id, queue_tid)
          |> Message.failed("connection error")

        authoritative_event = %{event | body: Map.put(event.body, "event_message", "newer")}
        authoritative_gen_tid = setup_generation_events([authoritative_event])
        authoritative_pointer = pointer_for(authoritative_event, authoritative_gen_tid, queue_tid)
        assert :ok = IngestEventQueue.reinsert_pointer(authoritative_pointer)

        telemetry_event = [:logflare, :ingest_event_queue, :requeue_deduplicated]
        ref = :telemetry_test.attach_event_handlers(self(), [telemetry_event])
        on_exit(fn -> :telemetry.detach(ref) end)

        log = capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed_message]) end)

        assert log =~ "Deduplicated 1 ClickHouse event(s) during retry requeue"

        assert_receive {^telemetry_event, ^ref, %{count: 1}, metadata}
        assert metadata == %{backend_type: :clickhouse, backend_id: backend.id}

        assert IngestEventQueue.lookup_event(old_gen_tid, event.id) == nil
        assert queued_pointer(queue_tid, event.id) == authoritative_pointer

        assert IngestEventQueue.lookup_event(
                 authoritative_pointer.tid,
                 authoritative_pointer.gen_event_id
               ) == authoritative_event
      end

      test "replaces a dangling same-ID pointer instead of deduplicating the retry", %{
        source: source,
        backend: backend
      } do
        event = build(:log_event, source: source, message: "claimed") |> Map.put(:retries, 0)
        old_gen_tid = setup_generation_events([event])
        queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])

        failed_message =
          event
          |> batch_message(old_gen_tid, backend.id, queue_tid)
          |> Message.failed("connection error")

        assert %EncodedRow{row: original_row} = failed_message.data

        stale_gen_tid = setup_generation_events([])
        dangling_pointer = pointer_for(event, stale_gen_tid, queue_tid)
        :ets.delete(stale_gen_tid)
        assert :ok = IngestEventQueue.reinsert_pointer(dangling_pointer)

        telemetry_event = [:logflare, :ingest_event_queue, :requeue_deduplicated]
        ref = :telemetry_test.attach_event_handlers(self(), [telemetry_event])
        on_exit(fn -> :telemetry.detach(ref) end)

        assert :ok = Pipeline.ack(:ack_ref, [], [failed_message])

        assert %LogEventPointer{retries: 1} = retry_pointer = queued_pointer(queue_tid, event.id)
        refute retry_pointer.tid == stale_gen_tid
        refute retry_pointer.gen_event_id == dangling_pointer.gen_event_id

        assert %EncodedRow{pointer: ^retry_pointer, row: ^original_row} =
                 IngestEventQueue.lookup_event(retry_pointer.tid, retry_pointer.gen_event_id)

        refute_receive {^telemetry_event, ^ref, _, _}
      end

      test "moves an unencoded retry into the current generation", %{
        source: source,
        backend: backend
      } do
        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        old_gen_tid = setup_generation_events([event])
        queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])
        queues_key = {:consolidated, backend.id}
        assert :ok = IngestEventQueue.new_generations([queues_key])
        current_gen_tid = IngestEventQueue.current_generation_tid(queues_key)

        failed_message = %Message{
          data: pointer_for(event, old_gen_tid, queue_tid),
          acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id}},
          status: {:failed, "connection error"}
        }

        assert :ok = Pipeline.ack(:ack_ref, [], [failed_message])

        assert %LogEventPointer{tid: ^current_gen_tid, retries: 1} =
                 retry_pointer = queued_pointer(queue_tid, event.id)

        assert IngestEventQueue.lookup_event(old_gen_tid, event.id) == nil

        assert %Logflare.LogEvent{retries: 1} =
                 IngestEventQueue.lookup_event(
                   retry_pointer.tid,
                   retry_pointer.gen_event_id
                 )
      end

      test "reroutes an encoded retry when its claiming producer queue is stale", %{
        source: source
      } do
        retry_backend_id = System.unique_integer([:positive])
        target_key = {:consolidated, retry_backend_id, self()}
        assert {:ok, target_tid} = IngestEventQueue.upsert_tid(target_key)

        stale_queue_tid = :ets.new(:test_pipeline_stale_retry_queue, [:set, :public])
        :ets.delete(stale_queue_tid)

        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        gen_tid = setup_generation_events([event])
        pointer = pointer_for(event, gen_tid, stale_queue_tid)
        original_row = <<1, 2, 3>>
        encoded = %EncodedRow{pointer: pointer, row: original_row}
        assert :ok = IngestEventQueue.replace_event(gen_tid, event.id, encoded)

        failed_message = %Message{
          data: encoded,
          acknowledger: {Pipeline, :ack_id, %{backend_id: retry_backend_id}},
          status: {:failed, "connection error"}
        }

        Mimic.stub(CircuitBreaker, :check, fn ^retry_backend_id -> :ok end)

        assert :ok = Pipeline.ack(:ack_ref, [], [failed_message])

        assert %LogEventPointer{retries: 1, queue_tid: ^target_tid} =
                 retry_pointer = queued_pointer(target_tid, event.id)

        assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

        assert %EncodedRow{pointer: ^retry_pointer, row: ^original_row} =
                 IngestEventQueue.lookup_event(retry_pointer.tid, retry_pointer.gen_event_id)
      end

      test "reports a queue-unavailable drop separately from a generation lookup miss", %{
        source: source
      } do
        retry_backend_id = System.unique_integer([:positive])
        stale_queue_tid = :ets.new(:test_pipeline_stale_retry_queue, [:set, :public])
        :ets.delete(stale_queue_tid)

        event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
        gen_tid = setup_generation_events([event])
        pointer = pointer_for(event, gen_tid, stale_queue_tid)
        encoded = %EncodedRow{pointer: pointer, row: <<1, 2, 3>>}
        assert :ok = IngestEventQueue.replace_event(gen_tid, event.id, encoded)

        failed_message = %Message{
          data: encoded,
          acknowledger: {Pipeline, :ack_id, %{backend_id: retry_backend_id}},
          status: {:failed, "connection error"}
        }

        dropped_event = [:logflare, :ingest_event_queue, :not_initialized, :dropped]
        lookup_miss_event = [:logflare, :ingest_event_queue, :requeue_lookup_miss]

        ref =
          :telemetry_test.attach_event_handlers(self(), [dropped_event, lookup_miss_event])

        on_exit(fn -> :telemetry.detach(ref) end)
        Mimic.stub(CircuitBreaker, :check, fn ^retry_backend_id -> :ok end)

        assert :ok = Pipeline.ack(:ack_ref, [], [failed_message])

        assert_receive {^dropped_event, ^ref, %{count: 1}, metadata}
        assert metadata.backend_type == :clickhouse
        assert metadata.backend_id == retry_backend_id
        refute_receive {^lookup_miss_event, ^ref, _, _}
        assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil
      end

      test "increments retry count on each encoded-row requeue", %{
        source: source,
        backend: backend
      } do
        max_retries = Pipeline.max_retries()
        initial_retries = max_retries - 1

        event =
          build(:log_event, source: source, message: "Test") |> Map.put(:retries, initial_retries)

        gen_tid = setup_generation_events([event])
        queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])

        failed_message =
          event
          |> batch_message(gen_tid, backend.id, queue_tid)
          |> Message.failed("connection error")

        Pipeline.ack(:ack_ref, [], [failed_message])

        assert %{retries: ^initial_retries} = event

        assert %LogEventPointer{retries: retries} =
                 retry_pointer =
                 queued_pointer(queue_tid, event.id)

        assert retries == initial_retries + 1
        assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

        assert %EncodedRow{pointer: ^retry_pointer} =
                 lookup_by_event_id(retry_pointer.tid, event.id)
      end

      test "handles mixed retriable and exhausted encoded rows", %{
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
        retriable_queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])
        exhausted_queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])

        failed_messages = [
          retriable_event
          |> batch_message(gen_tid, backend.id, retriable_queue_tid)
          |> Message.failed("error"),
          exhausted_event
          |> batch_message(gen_tid, backend.id, exhausted_queue_tid)
          |> Message.failed("error")
        ]

        log = capture_log(fn -> Pipeline.ack(:ack_ref, [], failed_messages) end)

        assert log =~ "Dropping 1 ClickHouse events: exhausted #{max_retries} retries"
        assert IngestEventQueue.lookup_event(gen_tid, exhausted_event.id) == nil
        assert queued_pointer(exhausted_queue_tid, exhausted_event.id) == nil

        assert %LogEventPointer{retries: 1} =
                 retry_pointer =
                 queued_pointer(retriable_queue_tid, retriable_event.id)

        assert IngestEventQueue.lookup_event(gen_tid, retriable_event.id) == nil

        assert %EncodedRow{pointer: ^retry_pointer} =
                 IngestEventQueue.lookup_event(retry_pointer.tid, retry_pointer.gen_event_id)
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
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert [%Message{status: {:failed, "Connection timeout"}}] = result
    end

    test "preserves rejected reasons when the valid-row insert also fails", %{
      context: context,
      source: source,
      backend: backend
    } do
      valid_event = build(:log_event, source: source, message: "valid row")

      invalid_event =
        build(:log_event, source: source, message: "invalid row")
        |> Map.put(:id, "not-a-uuid")

      missing_event = build(:log_event, source: source, message: "missing row")
      gen_tid = setup_generation_events([valid_event, invalid_event])

      messages = [
        batch_message(valid_event, gen_tid, backend.id),
        batch_message(invalid_event, gen_tid, backend.id),
        batch_message(missing_event, gen_tid, backend.id)
      ]

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        _opts ->
        {:error, "Connection timeout"}
      end)

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 3,
        trigger: :flush
      }

      result = Pipeline.handle_batch(:ch, messages, batch_info, context)

      statuses = Map.new(result, fn message -> {message_id(message), message.status} end)

      assert statuses[valid_event.id] == {:failed, "Connection timeout"}
      assert statuses[invalid_event.id] == {:failed, "invalid event UUID"}
      assert statuses[missing_event.id] == {:failed, :not_found}
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
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      assert [%Message{status: :ok}] =
               Pipeline.handle_batch(:ch, messages, batch_info, context)

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
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      assert [%Message{status: {:failed, "boom"}}] =
               Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert_received {:recorded_failure, ^expected_backend_id}
    end

    test "trips the circuit breaker immediately on a TOO_MANY_PARTS (252) failure", %{
      context: context,
      source: source,
      backend: backend
    } do
      test_pid = self()
      expected_backend_id = backend.id

      too_many_parts_error =
        "HTTP 500: Code: 252. DB::Exception: Too many parts (600 with average size of 1.00 MiB) in table."

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        _opts ->
        {:error, too_many_parts_error}
      end)

      # A 252 must trip immediately, not accumulate toward the failure-count threshold.
      Mimic.reject(CircuitBreaker, :record_failure, 1)

      Mimic.expect(CircuitBreaker, :trip, fn %{id: id} ->
        send(test_pid, {:tripped, id})
        :ok
      end)

      log_event = build(:log_event, source: source, message: "Test message")
      gen_tid = setup_generation_events([log_event])
      messages = [batch_message(log_event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      assert [%Message{status: {:failed, ^too_many_parts_error}}] =
               Pipeline.handle_batch(:ch, messages, batch_info, context)

      assert_received {:tripped, ^expected_backend_id}
    end
  end

  describe "ack/3 circuit breaker" do
    test "sheds a TOO_MANY_PARTS failure on its first acknowledgement", %{
      context: context,
      source: source,
      backend: backend
    } do
      too_many_parts_error = "HTTP 500: Code: 252. DB::Exception: Too many parts in table."

      Mimic.expect(ClickHouseAdaptor, :insert_log_events_compressed, fn _backend,
                                                                        _event_type,
                                                                        _compressed,
                                                                        _opts ->
        {:error, too_many_parts_error}
      end)

      Mimic.reject(CircuitBreaker, :record_failure, 1)
      Mimic.reject(IngestEventQueue, :add_to_table, 2)

      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
      gen_tid = setup_generation_events([event])
      messages = [batch_message(event, gen_tid, backend.id)]

      batch_info = %Broadway.BatchInfo{
        batcher: :ch,
        batch_key: {:log, @day_bucket},
        size: 1,
        trigger: :flush
      }

      assert [failed_message = %Message{status: {:failed, ^too_many_parts_error}}] =
               Pipeline.handle_batch(:ch, messages, batch_info, context)

      log = capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed_message]) end)

      assert log =~ "circuit breaker open"
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil
    end

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

    test "requeues encoded rows when the breaker is closed", %{
      source: source,
      backend: backend
    } do
      Mimic.stub(CircuitBreaker, :check, fn _backend_id -> :ok end)

      event = build(:log_event, source: source, message: "Test") |> Map.put(:retries, 0)
      gen_tid = setup_generation_events([event])
      queue_tid = :ets.new(:test_pipeline_retry_queue, [:set, :public])

      failed_message =
        event
        |> batch_message(gen_tid, backend.id, queue_tid)
        |> Message.failed("boom")

      Pipeline.ack(:ack_ref, [], [failed_message])

      assert %LogEventPointer{retries: 1} = retry_pointer = queued_pointer(queue_tid, event.id)
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

      assert %EncodedRow{pointer: ^retry_pointer} =
               IngestEventQueue.lookup_event(retry_pointer.tid, retry_pointer.gen_event_id)
    end
  end
end
