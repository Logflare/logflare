defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.PipelineTest do
  use Logflare.DataCase, async: false

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
    test "transforms event into Broadway message with correct acknowledger" do
      event = build(:log_event, message: "Test message")

      result = Pipeline.transform(event, [])

      assert %Message{
               data: ^event,
               acknowledger: {Pipeline, :ack_id, :ack_data}
             } = result
    end
  end

  describe "ack/3" do
    test "is a no-op that doesn't crash" do
      assert Pipeline.ack(:ack_ref, [:successful], [:failed]) == nil
    end

    test "handles empty successful and failed lists" do
      assert Pipeline.ack(:ack_ref, [], []) == nil
    end
  end
end
