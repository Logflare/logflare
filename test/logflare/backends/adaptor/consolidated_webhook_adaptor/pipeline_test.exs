defmodule Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.PipelineTest do
  use Logflare.DataCase

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Client
  alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.EncodedEvent
  alias Logflare.Backends.Adaptor.ConsolidatedWebhookAdaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.IngestEventQueue.LogEventPointer

  @dropped_event [:logflare, :ingest_event_queue, :retry_dropped]

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :consolidated_webhook,
        user: user,
        sources: [source],
        config: %{url: "https://example.com", format: "json", gzip: true, http: "http1"}
      )

    context = %{backend_id: backend.id, backend_token: backend.token, user_id: user.id}

    [source: source, backend: backend, context: context]
  end

  defp setup_generation_events(events) do
    tid = :ets.new(:test_consolidated_webhook_generation, [:set, :public])
    for event <- events, do: :ets.insert(tid, {event.id, event})
    tid
  end

  defp pointer_for(event, gen_tid) do
    %LogEventPointer{
      id: event.id,
      tid: gen_tid,
      gen_event_id: event.id,
      queue_tid: :ets.new(:test_consolidated_webhook_queue, [:set, :public]),
      size: :erlang.external_size(event.body),
      retries: event.retries || 0,
      event_type: event.event_type,
      day_bucket: event.day_bucket
    }
  end

  defp pointer_message(event, gen_tid, backend_id, opts) do
    ack_data = %{backend_id: backend_id, in_flight_ref: opts[:in_flight_ref]}

    %Message{
      data: pointer_for(event, gen_tid),
      acknowledger: {Pipeline, :ack_id, ack_data}
    }
  end

  defp encoded_message(event, gen_tid, backend_id, opts \\ []) do
    Pipeline.handle_message(:default, pointer_message(event, gen_tid, backend_id, opts), %{})
  end

  defp attach_dropped_handler do
    ref = :telemetry_test.attach_event_handlers(self(), [@dropped_event])
    on_exit(fn -> :telemetry.detach(ref) end)
    ref
  end

  describe "batch_size/1" do
    test "reads the stored config when the config is not typecast", %{backend: backend} do
      created = %{
        backend
        | config: nil,
          config_encrypted: %{url: "https://example.com", batch_size: 50}
      }

      assert Pipeline.batch_size(created) == 50
    end

    test "prefers the stored config over old typecast values", %{backend: backend} do
      updated = %{
        backend
        | config: %{url: "https://example.com", batch_size: 1_000},
          config_encrypted: %{"url" => "https://example.com", "batch_size" => 50}
      }

      assert Pipeline.batch_size(updated) == 50
    end

    test "uses the default when no batch size is stored", %{backend: backend} do
      assert Pipeline.batch_size(%{backend | config_encrypted: %{url: "https://example.com"}}) ==
               1_000
    end
  end

  describe "log_dropped/3" do
    test "logs one warning per backend per interval", %{backend: %{id: backend_id}} do
      first = capture_log(fn -> Pipeline.log_dropped(backend_id, 1, :rejected) end)
      second = capture_log(fn -> Pipeline.log_dropped(backend_id, 2, :rejected) end)
      other = capture_log(fn -> Pipeline.log_dropped(backend_id + 1, 3, :rejected) end)

      assert first =~ "Dropping 1 webhook events: rejected"
      assert second == ""
      assert other =~ "Dropping 3 webhook events: rejected"
    end
  end

  describe "join_payload/2" do
    test "joins encoded events into a JSON array" do
      assert Pipeline.join_payload(%{format: "json"}, [~s({"a":1}), ~s({"b":2})]) ==
               ~s([{"a":1},{"b":2}])

      assert Pipeline.join_payload(%{}, []) == "[]"
    end

    test "joins encoded events into NDJSON lines" do
      assert Pipeline.join_payload(%{format: "ndjson"}, [~s({"a":1}), ~s({"b":2})]) ==
               ~s({"a":1}\n{"b":2})
    end
  end

  describe "put_content_type/1" do
    test "sets the content type of the payload format" do
      assert %{headers: %{"content-type" => "application/json"}} =
               Pipeline.put_content_type(%{format: "json"})

      assert %{headers: %{"content-type" => "application/x-ndjson"}} =
               Pipeline.put_content_type(%{format: "ndjson", headers: nil})
    end

    test "keeps a content type from the user" do
      config = %{format: "json", headers: %{"Content-Type" => "text/plain"}}

      assert %{headers: %{"content-type" => "text/plain"}} = Pipeline.put_content_type(config)
    end
  end

  describe "handle_message/3" do
    test "encodes the event body and stores it in the generation store", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, source: source, message: "encode me")
      gen_tid = setup_generation_events([event])

      assert %Message{batcher: :http, data: %EncodedEvent{json: json, pointer: pointer}} =
               encoded_message(event, gen_tid, backend.id)

      assert %{"event_message" => "encode me"} = Jason.decode!(json)
      assert pointer.id == event.id
      assert %EncodedEvent{json: ^json} = IngestEventQueue.lookup_event(gen_tid, event.id)
    end

    test "reuses an event that was already encoded", %{source: source, backend: backend} do
      event = build(:log_event, source: source, message: "already encoded")
      gen_tid = setup_generation_events([event])
      stale_pointer = pointer_for(event, gen_tid)
      :ets.insert(gen_tid, {event.id, %EncodedEvent{pointer: stale_pointer, json: ~s({"a":1})}})

      message = pointer_message(event, gen_tid, backend.id, [])

      assert %Message{data: %EncodedEvent{json: ~s({"a":1}), pointer: pointer}} =
               Pipeline.handle_message(:default, message, %{})

      assert pointer == message.data
    end

    test "fails a message whose data is not a pointer", %{backend: backend} do
      message = %Message{
        data: :unexpected,
        acknowledger: {Pipeline, :ack_id, %{backend_id: backend.id, in_flight_ref: nil}}
      }

      assert %Message{status: {:failed, :not_found}} =
               Pipeline.handle_message(:default, message, %{})
    end

    test "fails one message when the event is missing", %{source: source, backend: backend} do
      event = build(:log_event, source: source)
      gen_tid = setup_generation_events([])

      assert %Message{status: {:failed, :not_found}} = encoded_message(event, gen_tid, backend.id)
    end

    test "fails one message when the body does not encode to JSON", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, source: source)
      event = %{event | body: Map.put(event.body, "bad", {:not, :json})}
      gen_tid = setup_generation_events([event])

      assert %Message{status: {:failed, {:rejected, :json_encode}}} =
               encoded_message(event, gen_tid, backend.id)
    end
  end

  describe "handle_batch/4" do
    setup %{source: source, backend: backend} do
      events = for n <- 1..3, do: build(:log_event, source: source, message: "event #{n}")
      gen_tid = setup_generation_events(events)
      messages = Enum.map(events, &encoded_message(&1, gen_tid, backend.id))

      [messages: messages, batch_info: %Broadway.BatchInfo{size: 3, trigger: :timeout}]
    end

    test "sends one request with all events of the batch", %{
      messages: messages,
      batch_info: batch_info,
      context: context,
      backend: backend
    } do
      expect(Client, :send, fn req ->
        assert req[:url] == "https://example.com"
        assert req[:gzip] == true
        assert req[:http] == "http1"
        assert req[:headers]["content-type"] == "application/json"
        assert req[:opts][:metadata]["backend_id"] == backend.id

        assert ["event 1", "event 2", "event 3"] =
                 req[:body] |> Jason.decode!() |> Enum.map(& &1["event_message"])

        {:ok, %Tesla.Env{status: 204}}
      end)

      result = Pipeline.handle_batch(:http, messages, batch_info, context)

      assert result == messages
    end

    test "marks the batch as retriable for transient failures", %{
      messages: messages,
      batch_info: batch_info,
      context: context
    } do
      responses = [
        {:ok, %Tesla.Env{status: 500}},
        {:ok, %Tesla.Env{status: 429}},
        {:ok, %Tesla.Env{status: 408}},
        {:error, :timeout}
      ]

      for response <- responses do
        expect(Client, :send, fn _req -> response end)

        result = Pipeline.handle_batch(:http, messages, batch_info, context)

        assert Enum.all?(result, &match?(%Message{status: {:failed, {:retriable, _}}}, &1))
      end
    end

    test "marks the batch as rejected for a client error", %{
      messages: messages,
      batch_info: batch_info,
      context: context
    } do
      expect(Client, :send, fn _req -> {:ok, %Tesla.Env{status: 400}} end)

      result = Pipeline.handle_batch(:http, messages, batch_info, context)

      assert Enum.all?(result, &match?(%Message{status: {:failed, {:rejected, 400}}}, &1))
    end

    test "rejects the batch when the backend does not exist", %{
      messages: messages,
      batch_info: batch_info,
      context: context
    } do
      reject(&Client.send/1)

      result = Pipeline.handle_batch(:http, messages, batch_info, %{context | backend_id: 0})

      assert Enum.all?(
               result,
               &match?(%Message{status: {:failed, {:rejected, :backend_not_found}}}, &1)
             )
    end
  end

  describe "ack/3" do
    test "returns :ok when both lists are empty" do
      assert :ok = Pipeline.ack(:ack_ref, [], [])
    end

    test "deletes successful events and releases the in-flight count", %{
      source: source,
      backend: backend
    } do
      event = build(:log_event, source: source)
      gen_tid = setup_generation_events([event])
      in_flight_ref = :atomics.new(1, signed: true)
      :atomics.put(in_flight_ref, 1, 5)

      message = encoded_message(event, gen_tid, backend.id, in_flight_ref: in_flight_ref)

      assert :ok = Pipeline.ack(:ack_ref, [message], [])
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil
      assert :atomics.get(in_flight_ref, 1) == 4
    end

    test "drops the events of a failed request", %{source: source, backend: backend} do
      event = build(:log_event, source: source)
      gen_tid = setup_generation_events([event])
      ref = attach_dropped_handler()

      failed = event |> encoded_message(gen_tid, backend.id) |> Message.failed({:retriable, 503})

      log = capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed]) end)

      assert log =~ "Dropping 1 webhook events: request_failed"
      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil

      assert_receive {@dropped_event, ^ref, %{count: 1},
                      %{reason: :request_failed, backend_type: :consolidated_webhook}}
    end

    test "drops rejected events", %{source: source, backend: backend} do
      event = build(:log_event, source: source)
      gen_tid = setup_generation_events([event])
      ref = attach_dropped_handler()

      failed = event |> encoded_message(gen_tid, backend.id) |> Message.failed({:rejected, 400})

      capture_log(fn -> Pipeline.ack(:ack_ref, [], [failed]) end)

      assert IngestEventQueue.lookup_event(gen_tid, event.id) == nil
      assert_receive {@dropped_event, ^ref, %{count: 1}, %{reason: :rejected}}
    end

    test "ignores a message whose event is missing", %{source: source, backend: backend} do
      event = build(:log_event, source: source)
      gen_tid = setup_generation_events([])
      ref = attach_dropped_handler()

      failed = encoded_message(event, gen_tid, backend.id)

      assert :ok = Pipeline.ack(:ack_ref, [], [failed])
      refute_receive {@dropped_event, ^ref, _, _}
    end
  end
end
