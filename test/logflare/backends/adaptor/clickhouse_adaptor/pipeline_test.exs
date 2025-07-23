defmodule Logflare.Backends.Adaptor.ClickhouseAdaptor.PipelineTest do
  use Logflare.DataCase
  import Mimic

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor.Pipeline
  alias Logflare.Backends.BufferProducer
  alias Logflare.Backends.SourceRegistry
  alias Logflare.LogEvent

  setup :verify_on_exit!

  describe "child_spec/1" do
    test "returns proper child specification" do
      spec = Pipeline.child_spec(:some_arg)

      assert spec.id == Pipeline
      assert spec.start == {Pipeline, :start_link, [:some_arg]}
    end
  end

  describe "start_link/1" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      adaptor_state = %ClickhouseAdaptor{
        source: source,
        backend: backend,
        pipeline_name: {:via, Registry, {SourceRegistry, {source.id, backend.id, Pipeline}}},
        connection_name: {:via, Registry, {SourceRegistry, {source.id, backend.id, :connection}}}
      }

      %{adaptor_state: adaptor_state}
    end

    test "calls Broadway.start_link with correct configuration", %{adaptor_state: adaptor_state} do
      Broadway
      |> expect(:start_link, fn module, opts ->
        assert module == Pipeline
        assert opts[:name] == adaptor_state.pipeline_name
        assert opts[:context] == adaptor_state

        producer_config = opts[:producer]

        assert producer_config[:module] ==
                 {BufferProducer,
                  [
                    source_id: adaptor_state.source.id,
                    backend_id: adaptor_state.backend.id
                  ]}

        assert producer_config[:transformer] == {Pipeline, :transform, []}
        assert producer_config[:concurrency] == 1

        # Verify processors configuration
        assert opts[:processors][:default][:concurrency] == 5
        assert opts[:processors][:default][:min_demand] == 1

        # Verify batchers configuration
        assert opts[:batchers][:ch][:concurrency] == 5
        assert opts[:batchers][:ch][:batch_size] == 350

        {:ok, self()}
      end)

      assert {:ok, _pid} = Pipeline.start_link(adaptor_state)
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
    test "routes all messages to :ch batcher" do
      message = %Message{
        data: build(:log_event),
        acknowledger: {Pipeline, :ack_id, :ack_data}
      }

      adaptor_state = %ClickhouseAdaptor{}

      result = Pipeline.handle_message(:default, message, adaptor_state)

      assert %Message{batcher: :ch} = result
      assert result.data == message.data
    end
  end

  describe "handle_batch/4" do
    setup do
      user = insert(:user)
      source = insert(:source, user: user)
      backend = insert(:backend, type: :clickhouse)

      connection_name =
        {:via, Registry, {SourceRegistry, {source.id, backend.id, :connection}}}

      context = %{
        source: source,
        backend: backend,
        connection_name: connection_name
      }

      %{context: context, source: source, backend: backend, connection_name: connection_name}
    end

    test "extracts events from messages and calls adaptor functions", %{
      context: context,
      source: source,
      backend: backend,
      connection_name: connection_name
    } do
      log_event1 = build(:log_event, source: source, message: "Test message 1")
      log_event2 = build(:log_event, source: source, message: "Test message 2")

      messages = [
        %Message{data: log_event1, acknowledger: {Pipeline, :ack_id, :ack_data}},
        %Message{data: log_event2, acknowledger: {Pipeline, :ack_id, :ack_data}}
      ]

      ClickhouseAdaptor
      |> expect(:insert_log_events, fn conn_name, {source_arg, backend_arg}, events ->
        assert conn_name == connection_name
        assert source_arg == source
        assert backend_arg == backend
        assert length(events) == 2
        assert Enum.all?(events, &is_struct(&1, LogEvent))
        :ok
      end)

      result = Pipeline.handle_batch(:ch, messages, %{}, context)

      assert result == messages
    end

    test "handles empty messages list", %{context: context} do
      ClickhouseAdaptor
      |> expect(:insert_log_events, fn _, _, events ->
        assert events == []
        :ok
      end)

      result = Pipeline.handle_batch(:ch, [], %{}, context)

      assert result == []
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
