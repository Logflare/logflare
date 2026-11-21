defmodule Logflare.Backends.Adaptor.BigQueryV2Adaptor.PipelineTest do
  use Logflare.DataCase, async: false

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.GoogleApiClient
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor
  alias Logflare.Backends.Adaptor.BigQueryV2Adaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue

  @config %{project_id: "my-project-id", dataset_id: "my_dataset"}

  setup :set_mimic_global

  setup do
    insert(:plan, name: "Free")
    user = insert(:user)
    source = insert(:source, user: user)
    backend = insert(:backend, type: :bigquery_v2, config: @config, user: user)

    [source: source, backend: %{backend | config: @config}]
  end

  describe "ingestion" do
    test "appends mapped events to otel_logs", %{source: source, backend: backend} do
      test_pid = self()

      stub(BigQueryV2Adaptor, :insert_rows, fn backend, event_type, rows ->
        send(test_pid, {:insert_rows, event_type, rows})
        call_original(BigQueryV2Adaptor, :insert_rows, [backend, event_type, rows])
      end)

      stub(GoogleApiClient, :append_rows, fn {:arrow, arrow_data}, context, table ->
        send(test_pid, {:append_rows, arrow_data, context, table})
        :ok
      end)

      start_supervised!({BigQueryV2Adaptor, backend})

      events = [
        build(:log_event, source: source, message: "first", name: "bob", user: %{"id" => 5}),
        build(:log_event, source: source, message: "second", metadata: %{"level" => "warn"})
      ]

      :ok = IngestEventQueue.add_to_table({:consolidated, backend.id}, events)

      assert_receive {:insert_rows, :log, rows}, 5_000
      assert_receive {:append_rows, [_record_batch], context, "otel_logs"}, 5_000
      assert context == [project_id: "my-project-id", dataset_id: "my_dataset"]

      rows = Enum.map(rows, &Jason.decode!/1)
      assert Enum.map(rows, & &1["event_message"]) |> Enum.sort() == ["first", "second"]

      first = Enum.find(rows, &(&1["event_message"] == "first"))
      assert first["source_uuid"] == Atom.to_string(source.token)
      assert first["source_name"] == source.name

      assert %{"name" => "bob", "user.id" => "5"} = first["log_attributes"]

      second = Enum.find(rows, &(&1["event_message"] == "second"))
      assert %{"level" => "warn"} = second["log_attributes"]
    end

    test "routes metric and trace events to their own tables", %{
      source: source,
      backend: backend
    } do
      test_pid = self()

      stub(BigQueryV2Adaptor, :insert_rows, fn backend, event_type, rows ->
        send(test_pid, {:insert_rows, event_type, rows})
        call_original(BigQueryV2Adaptor, :insert_rows, [backend, event_type, rows])
      end)

      stub(GoogleApiClient, :append_rows, fn {:arrow, _arrow_data}, _context, table ->
        send(test_pid, {:append_rows, table})
        :ok
      end)

      start_supervised!({BigQueryV2Adaptor, backend})

      span =
        build(:log_event,
          source: source,
          span_name: "GET /api",
          trace_id: "t1",
          span_id: "s1",
          events: [%{"name" => "exception", "attributes" => %{"exception.message" => "boom"}}]
        )

      metric = build(:log_event, source: source, metric_name: "http.duration", sum: 12.5)

      :ok =
        IngestEventQueue.add_to_table({:consolidated, backend.id}, [
          %{span | event_type: :trace},
          %{metric | event_type: :metric}
        ])

      assert_receive {:insert_rows, :trace, [trace_row]}, 5_000
      assert_receive {:insert_rows, :metric, [metric_row]}, 5_000
      assert_receive {:append_rows, "otel_traces"}, 5_000
      assert_receive {:append_rows, "otel_metrics"}, 5_000

      assert %{"span_name" => "GET /api", "events.name" => ["exception"]} =
               Jason.decode!(trace_row)

      assert %{"metric_name" => "http.duration", "sum" => 12.5} = Jason.decode!(metric_row)
    end

    @tag capture_log: true
    test "retries a failed append once, then drops the events", %{
      source: source,
      backend: backend
    } do
      test_pid = self()

      stub(GoogleApiClient, :append_rows, fn _rows, _context, _table ->
        send(test_pid, :append_attempt)
        {:error, :unavailable}
      end)

      start_supervised!({BigQueryV2Adaptor, backend})

      :ok =
        IngestEventQueue.add_to_table({:consolidated, backend.id}, [
          build(:log_event, source: source)
        ])

      assert_receive :append_attempt, 5_000
      assert_receive :append_attempt, 5_000
      refute_receive :append_attempt, 3_000
    end
  end

  describe "handle_message/3" do
    test "fails events of a type without a table", %{source: source, backend: backend} do
      event = %{build(:log_event, source: source) | event_type: :metric}
      message = Pipeline.transform(event, backend_id: backend.id)

      assert %Message{status: {:failed, {:unsupported_event_type, :metric}}} =
               Pipeline.handle_message(:default, message, %{mapper_configs: %{}})
    end
  end

  describe "ack/3" do
    @tag capture_log: true
    test "drops events that cannot be retried", %{source: source, backend: backend} do
      reject(IngestEventQueue, :add_to_table, 2)

      message =
        build(:log_event, source: source)
        |> Pipeline.transform(backend_id: backend.id)
        |> Message.failed({:unsupported_event_type, :metric})

      assert :ok = Pipeline.ack(:ack_id, [], [message])
    end

    @tag capture_log: true
    test "requeues events with an incremented retry count", %{source: source, backend: backend} do
      backend_id = backend.id
      test_pid = self()

      expect(IngestEventQueue, :add_to_table, fn {:consolidated, ^backend_id}, events ->
        send(test_pid, {:requeued, events})
        :ok
      end)

      message =
        build(:log_event, source: source)
        |> Pipeline.transform(backend_id: backend.id)
        |> Message.failed(:unavailable)

      Pipeline.ack(:ack_id, [], [message])

      assert_received {:requeued, [%{retries: 1}]}
    end
  end
end
