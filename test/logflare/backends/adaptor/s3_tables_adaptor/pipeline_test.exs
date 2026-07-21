defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.CatalogManager
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Native
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue

  @backend_id 123
  @context %{backend_id: @backend_id}
  # arbitrary day bucket value — the pipeline only passes it through telemetry
  @day_bucket 20_594

  defp message(event) do
    %Message{data: event, acknowledger: {Pipeline, :ack_id, %{backend_id: @backend_id}}}
  end

  defp batch_info(batch_key, size) do
    %Broadway.BatchInfo{
      batcher: :s3_tables,
      batch_key: batch_key,
      size: size,
      trigger: :flush
    }
  end

  test "handle_message/3" do
    for event_type <- [:log, :metric, :trace] do
      event = build(:log_event) |> Map.put(:event_type, event_type)

      result = Pipeline.handle_message(:default, message(event), @context)

      assert %Message{batcher: :s3_tables, batch_key: {^event_type, day_bucket}} = result
      assert day_bucket == event.day_bucket
    end
  end

  describe "handle_batch/4" do
    setup do
      catalog = make_ref()
      Mimic.stub(CatalogManager, :fetch_catalog, fn @backend_id -> {:ok, catalog} end)
      [catalog: catalog]
    end

    test "NDJSON rows append", %{catalog: catalog} do
      event1 = build(:log_event, message: "Test message 1", metadata: %{"level" => "info"})
      event2 = build(:log_event, message: "Test message 2")
      messages = [message(event1), message(event2)]

      test_pid = self()

      Mimic.expect(Native, :append_batch, fn ^catalog, table_name, ndjson ->
        send(test_pid, {:appended, table_name, ndjson})
        {:ok, %{row_count: 2, data_files: 1}}
      end)

      result =
        Pipeline.handle_batch(:s3_tables, messages, batch_info({:log, @day_bucket}, 2), @context)

      assert result == messages
      assert_receive {:appended, "otel_logs", ndjson}

      rows =
        ndjson
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode!/1)

      assert [row1, row2] = rows
      assert row1["id"] == event1.id
      assert row1["source_uuid"] == Atom.to_string(event1.source_uuid)
      assert row1["event_message"] == "Test message 1"
      assert is_binary(row1["mapping_config_id"])
      assert is_map(row1["log_attributes"])
      assert row2["event_message"] == "Test message 2"

      # mapper scales body timestamps (µs) to ns; the NIF scales them back down
      assert row1["timestamp"] == event1.body["timestamp"] * 1000
    end

    test "metric and trace batches routing" do
      test_pid = self()

      Mimic.expect(Native, :append_batch, 2, fn _catalog, table_name, _ndjson ->
        send(test_pid, {:appended, table_name})
        {:ok, %{row_count: 1, data_files: 1}}
      end)

      for {event_type, table_name} <- [metric: "otel_metrics", trace: "otel_traces"] do
        event = build(:log_event) |> Map.put(:event_type, event_type)

        Pipeline.handle_batch(
          :s3_tables,
          [message(event)],
          batch_info({event_type, @day_bucket}, 1),
          @context
        )

        assert_receive {:appended, ^table_name}
      end
    end

    test "on append error" do
      messages = [message(build(:log_event)), message(build(:log_event))]

      Mimic.expect(Native, :append_batch, fn _catalog, _table_name, _ndjson ->
        {:error, :commit_conflict}
      end)

      log =
        capture_log(fn ->
          result =
            Pipeline.handle_batch(
              :s3_tables,
              messages,
              batch_info({:log, @day_bucket}, 2),
              @context
            )

          assert [
                   %Message{status: {:failed, :commit_conflict}},
                   %Message{status: {:failed, :commit_conflict}}
                 ] = result
        end)

      assert log =~ "S3 Tables append failed"
    end

    test "when catalog is not provisioned" do
      Mimic.stub(CatalogManager, :fetch_catalog, fn @backend_id -> {:error, :not_provisioned} end)
      Mimic.reject(Native, :append_batch, 3)

      messages = [message(build(:log_event))]

      capture_log(fn ->
        assert [%Message{status: {:failed, :not_provisioned}}] =
                 Pipeline.handle_batch(
                   :s3_tables,
                   messages,
                   batch_info({:log, @day_bucket}, 1),
                   @context
                 )
      end)
    end

    test "append telemetry" do
      Mimic.expect(Native, :append_batch, fn _catalog, _table_name, _ndjson ->
        {:ok, %{row_count: 1, data_files: 1}}
      end)

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :pipeline, :handle_batch],
        [:logflare, :backends, :s3_tables, :append]
      ])

      Pipeline.handle_batch(
        :s3_tables,
        [message(build(:log_event))],
        batch_info({:log, @day_bucket}, 1),
        @context
      )

      assert_receive {[:logflare, :backends, :pipeline, :handle_batch], _ref,
                      %{batch_size: 1, batch_trigger: :flush},
                      %{
                        backend_type: :s3_tables,
                        backend_id: @backend_id,
                        event_type: :log,
                        day_bucket: @day_bucket
                      }}

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref,
                      %{duration_us: _, row_count: 1, data_files: 1},
                      %{status: :ok, backend_id: @backend_id, event_type: :log}}
    end

    test "append telemetry on failure" do
      Mimic.expect(Native, :append_batch, fn _catalog, _table_name, _ndjson ->
        {:error, :timeout}
      end)

      :telemetry_test.attach_event_handlers(self(), [
        [:logflare, :backends, :s3_tables, :append]
      ])

      capture_log(fn ->
        Pipeline.handle_batch(
          :s3_tables,
          [message(build(:log_event))],
          batch_info({:log, @day_bucket}, 1),
          @context
        )
      end)

      assert_receive {[:logflare, :backends, :s3_tables, :append], _ref, %{duration_us: _},
                      %{status: :error, reason: :timeout}}
    end
  end

  describe "ack/3" do
    test "returns :ok when failed list is empty" do
      assert Pipeline.ack(:ack_ref, [], []) == :ok
    end

    test "requeues retriable messages" do
      event = build(:log_event) |> Map.put(:retries, 0)

      failed = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, %{backend_id: @backend_id}},
        status: {:failed, :commit_conflict}
      }

      test_pid = self()

      Mimic.expect(IngestEventQueue, :delete_batch, fn {:consolidated, @backend_id}, events ->
        send(test_pid, {:deleted_batch, events})
        :ok
      end)

      Mimic.expect(IngestEventQueue, :add_to_table, fn {:consolidated, @backend_id}, events ->
        send(test_pid, {:requeued, events})
        :ok
      end)

      Pipeline.ack(:ack_ref, [], [failed])

      assert_receive {:deleted_batch, [_event]}
      assert_receive {:requeued, [requeued_event]}
      assert requeued_event.retries == 1
    end

    test "drops exhausted messages" do
      max_retries = Pipeline.max_retries()
      event = build(:log_event) |> Map.put(:retries, max_retries)

      failed = %Message{
        data: event,
        acknowledger: {Pipeline, :ack_id, %{backend_id: @backend_id}},
        status: {:failed, :commit_conflict}
      }

      test_pid = self()

      Mimic.expect(IngestEventQueue, :delete_batch, fn {:consolidated, @backend_id}, events ->
        send(test_pid, {:deleted_batch, events})
        :ok
      end)

      Mimic.reject(IngestEventQueue, :add_to_table, 2)

      log = capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed]) end)

      assert log =~ "Dropping 1 S3 Tables events: exhausted #{max_retries} retries"
      assert_receive {:deleted_batch, [dropped_event]}
      assert dropped_event.id == event.id
    end

    test "mixed retriable and exhausted messages" do
      max_retries = Pipeline.max_retries()

      retriable = build(:log_event, message: "Retriable") |> Map.put(:retries, 0)
      exhausted = build(:log_event, message: "Exhausted") |> Map.put(:retries, max_retries)

      failed_messages =
        Enum.map([retriable, exhausted], fn event ->
          %Message{
            data: event,
            acknowledger: {Pipeline, :ack_id, %{backend_id: @backend_id}},
            status: {:failed, :error}
          }
        end)

      test_pid = self()

      Mimic.expect(IngestEventQueue, :delete_batch, 2, fn {:consolidated, @backend_id}, events ->
        send(test_pid, {:deleted_batch, Enum.map(events, & &1.id)})
        :ok
      end)

      Mimic.expect(IngestEventQueue, :add_to_table, fn {:consolidated, @backend_id}, events ->
        send(test_pid, {:requeued, events})
        :ok
      end)

      capture_log(fn -> Pipeline.ack(:ack_ref, [], failed_messages) end)

      assert_receive {:requeued, [requeued_event]}
      assert requeued_event.retries == 1
      assert requeued_event.body["event_message"] == "Retriable"
    end
  end
end
