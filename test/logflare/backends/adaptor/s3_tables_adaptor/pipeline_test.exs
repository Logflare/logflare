defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Mapper.ConfigStore

  @backend_id 123

  defp context do
    mapper_configs =
      Map.new([:log, :metric, :trace], fn event_type ->
        {:ok, compiled, config_id} = ConfigStore.get_compiled(event_type, :ndjson)
        {event_type, %{compiled: compiled, config_id: config_id}}
      end)

    %{backend_id: @backend_id, mapper_configs: mapper_configs}
  end

  defp message(event) do
    %Message{data: event, acknowledger: {Pipeline, :ack_id, %{backend_id: @backend_id}}}
  end

  defp failed_message(event, reason, backend_id) do
    %Message{
      data: event,
      acknowledger: {Pipeline, :ack_id, %{backend_id: backend_id}},
      status: {:failed, reason}
    }
  end

  describe "handle_message/3" do
    test "every event type" do
      for event_type <- [:log, :metric, :trace] do
        event = build(:log_event) |> Map.put(:event_type, event_type)

        result = Pipeline.handle_message(:default, message(event), context())

        assert %Message{batcher: :s3_tables, batch_key: {^event_type, day_bucket}} = result
        assert %Message{data: {^event, row}} = result
        assert day_bucket == event.day_bucket
        assert String.ends_with?(row, "\n")
      end
    end

    test "event with non-UTF-8 envelope field" do
      event =
        build(:log_event)
        |> Map.put(:event_type, :log)
        |> Map.put(:source_name, <<0xFF, 0xFE>>)

      assert %Message{status: {:failed, {:encode_error, reason}}} =
               Pipeline.handle_message(:default, message(event), context())

      assert reason =~ "UTF-8"
    end
  end

  describe "ack/3" do
    setup do
      backend_id = System.unique_integer([:positive])
      IngestEventQueue.upsert_tid({:consolidated, backend_id, nil})
      IngestEventQueue.current_generation_tid({:consolidated, backend_id})
      [backend_id: backend_id]
    end

    test "empty failed list" do
      assert Pipeline.ack(:ack_ref, [], []) == :ok
    end

    test "encode-failed message", %{backend_id: backend_id} do
      event = build(:log_event) |> Map.put(:retries, 0)
      IngestEventQueue.add_to_table({:consolidated, backend_id}, [event])

      log =
        capture_log(fn ->
          Pipeline.ack(:ack_ref, [], [failed_message(event, {:encode_error, "boom"}, backend_id)])
        end)

      assert log =~ "encoding failed"
      assert IngestEventQueue.total_pending({:consolidated, backend_id}) == 0
    end

    test "retriable message", %{backend_id: backend_id} do
      event = build(:log_event) |> Map.put(:retries, 0)
      IngestEventQueue.add_to_table({:consolidated, backend_id}, [event])

      Pipeline.ack(:ack_ref, [], [failed_message(event, :commit_conflict, backend_id)])

      assert IngestEventQueue.total_pending({:consolidated, backend_id}) == 1
    end

    test "exhausted message", %{backend_id: backend_id} do
      event = build(:log_event) |> Map.put(:retries, Pipeline.max_retries())
      IngestEventQueue.add_to_table({:consolidated, backend_id}, [event])

      log =
        capture_log(fn ->
          Pipeline.ack(:ack_ref, [], [failed_message(event, :commit_conflict, backend_id)])
        end)

      assert log =~ "Dropping 1 S3 Tables events: exhausted #{Pipeline.max_retries()} retries"
      assert IngestEventQueue.total_pending({:consolidated, backend_id}) == 0
    end

    test "mixed retriable and exhausted messages", %{backend_id: backend_id} do
      retriable = build(:log_event, message: "Retriable") |> Map.put(:retries, 0)

      exhausted =
        build(:log_event, message: "Exhausted") |> Map.put(:retries, Pipeline.max_retries())

      IngestEventQueue.add_to_table({:consolidated, backend_id}, [retriable, exhausted])

      capture_log(fn ->
        Pipeline.ack(:ack_ref, [], [
          failed_message(retriable, :error, backend_id),
          failed_message(exhausted, :error, backend_id)
        ])
      end)

      assert IngestEventQueue.total_pending({:consolidated, backend_id}) == 1
    end
  end
end
