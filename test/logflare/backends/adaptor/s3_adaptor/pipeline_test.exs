defmodule Logflare.Backends.Adaptor.S3Adaptor.PipelineTest do
  use Logflare.DataCase, async: false

  import ExUnit.CaptureLog

  alias Broadway.Message
  alias Logflare.Backends.Adaptor.S3Adaptor.Pipeline
  alias Logflare.Backends.IngestEventQueue

  setup do
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :s3,
        sources: [source],
        config: %{
          s3_bucket: "my-bucket",
          storage_region: "us-east-1",
          access_key_id: "AKID",
          secret_access_key: "SECRET",
          batch_timeout: 1_000
        }
      )

    event = build(:log_event, source: source)
    opts = [source_id: source.id, backend_id: backend.id]
    message = Pipeline.transform(event, opts)

    [source: source, backend: backend, event: event, message: message, opts: opts]
  end

  test "transform carries the source and backend into acknowledgements", %{
    source: source,
    backend: backend,
    event: event,
    opts: opts
  } do
    assert %Message{
             data: ^event,
             acknowledger: {Pipeline, {source_id, backend_id}, nil}
           } = Pipeline.transform(event, opts)

    assert source_id == source.id
    assert backend_id == backend.id
  end

  describe "handle_batch/4" do
    test "returns successful messages after an upload", %{message: message} do
      expect(ExAws, :request, fn _operation, _opts -> {:ok, %{status_code: 200}} end)

      assert [^message] =
               Pipeline.handle_batch(
                 :s3,
                 [message],
                 %Broadway.BatchInfo{},
                 batch_context(message)
               )
    end

    test "marks every message failed when an upload fails", %{message: message} do
      reason = {:http_error, 503, "unavailable"}
      expect(ExAws, :request, fn _operation, _opts -> {:error, reason} end)

      assert [%Message{status: {:failed, ^reason}}] =
               Pipeline.handle_batch(
                 :s3,
                 [message],
                 %Broadway.BatchInfo{},
                 batch_context(message)
               )
    end
  end

  describe "ack/3" do
    test "requeues a failed event once", %{
      source: source,
      backend: backend,
      event: event,
      message: message
    } do
      failed = Message.failed(%{message | data: %{event | is_popped: true}}, :timeout)

      expect(IngestEventQueue, :add_to_table, fn {source_id, backend_id}, [requeued] ->
        assert source_id == source.id
        assert backend_id == backend.id
        assert requeued.id == event.id
        assert requeued.retries == 1
        refute requeued.is_popped
        :ok
      end)

      assert :ok = Pipeline.ack({source.id, backend.id}, [], [failed])
    end

    test "drops an event after its retry is exhausted", %{
      source: source,
      backend: backend,
      event: event,
      message: message
    } do
      failed = Message.failed(%{message | data: %{event | retries: 1}}, :timeout)
      Mimic.reject(IngestEventQueue, :add_to_table, 2)

      log =
        capture_log(fn ->
          assert :ok = Pipeline.ack({source.id, backend.id}, [], [failed])
        end)

      assert log =~ "dropped 1 events after 1 retry"
    end

    test "does not touch the queue when every message succeeded", %{
      source: source,
      backend: backend,
      message: message
    } do
      Mimic.reject(IngestEventQueue, :add_to_table, 2)

      assert :ok = Pipeline.ack({source.id, backend.id}, [message], [])
    end
  end

  defp batch_context(%Message{acknowledger: {Pipeline, {source_id, backend_id}, nil}}) do
    %{source_id: source_id, backend_id: backend_id}
  end
end
