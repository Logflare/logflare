defmodule Logflare.Backends.Spool.ProducerPipelineTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Broadway.Message
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.ProducerPipeline
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.LogEvent
  alias Logflare.TestUtils

  setup :set_mimic_global

  @max_spool_file_size 32 * 1024 * 1024
  @early_flush_file_size 12 * 1024 * 1024

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    :ok
  end

  defp message(size), do: %{data: {Ecto.UUID.generate(), :fake_tid, size}}

  # Builds a real ETS-backed {id, tid, size} message via the same
  # IngestEventQueue.upsert_tid/1 + add_to_table/2 path real ingestion uses,
  # matching what BufferProducer's id_passing: true path actually hands to
  # handle_batch/4 — unlike message/1 above, this is a real log event the
  # upload functions can look up and serialize. Going through the real
  # insertion (add_to_table/2) and lookup (IngestEventQueue.lookup_id/2)
  # helpers, rather than hand-rolling or destructuring the ETS tuple
  # ourselves, means a row-shape change there shows up as a test failure
  # here instead of silently drifting out of sync.
  defp log_event_message(body \\ %{"message" => "hello"}, via_rule_id \\ nil) do
    log_event = %LogEvent{
      id: Ecto.UUID.generate(),
      source_id: 1,
      body: body,
      event_type: :log,
      ingested_at: DateTime.utc_now(),
      valid: true,
      drop: false,
      via_rule_id: via_rule_id
    }

    key = {:spool_producer, nil, self()}
    {:ok, tid} = IngestEventQueue.upsert_tid(key)
    :ok = IngestEventQueue.add_to_table(key, [log_event])

    {_id, _status, _event, byte_size} = IngestEventQueue.lookup_id(tid, log_event.id)

    %Message{
      data: {log_event.id, tid, byte_size},
      acknowledger: {ProducerPipeline, :no_ack_ref, :ack_data}
    }
  end

  defp handle_batch_context(overrides \\ []) do
    [
      bucket: "test-bucket",
      partitions: 1,
      compress: true,
      format: :ndjson,
      queue_ref: "projects/p/topics/t",
      storage_mod: StorageMod,
      queue_mod: QueueMod
    ]
    |> Keyword.merge(overrides)
    |> Map.new()
  end

  defp not_throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 1.0,
      spool_max_ets_percent: 1.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)
  end

  defp throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)
  end

  describe "spool_batch_size_splitter/0 when not throttled" do
    test "continues accumulating under the normal 32MB budget" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, {_count, remaining}} = reducer.(message(10 * 1024 * 1024), initial)
      assert remaining == @max_spool_file_size - 10 * 1024 * 1024
    end

    test "emits once accumulated size would exceed the normal 32MB budget" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, acc} = reducer.(message(20 * 1024 * 1024), initial)
      assert {:emit, {_count, :pending}} = reducer.(message(15 * 1024 * 1024), acc)
    end
  end

  describe "spool_batch_size_splitter/0 when throttled" do
    test "emits at the smaller early-flush budget instead of the normal 32MB one" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      # 15MB alone wouldn't trip the normal 32MB budget, but does trip the 12MB one.
      assert {:emit, {_count, :pending}} = reducer.(message(15 * 1024 * 1024), initial)
    end

    test "continuing under the early-flush budget still tracks the correct remaining bytes" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, {_count, remaining}} = reducer.(message(5 * 1024 * 1024), initial)
      assert remaining == @early_flush_file_size - 5 * 1024 * 1024
    end

    test "the budget decided for a batch stays locked in even if throttling clears mid-batch" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      # First message decides the budget for this whole batch: 12MB (throttled).
      assert {:cont, acc} = reducer.(message(5 * 1024 * 1024), initial)

      # Flip to "not throttled" and wait for MemoryMonitor to genuinely refresh —
      # if the reducer re-checked per message, this would flip its behavior.
      Application.put_env(:logflare, :spool,
        spool_memory_limit_percent: 1.0,
        spool_max_ets_percent: 1.0
      )

      Process.sleep(1_100)
      assert MemoryMonitor.throttled?() == false

      # 5MB + 8MB = 13MB, over the locked-in 12MB budget (would NOT emit under
      # a freshly-rechecked 32MB budget) — proves the decision wasn't re-evaluated.
      assert {:emit, {_count, :pending}} = reducer.(message(8 * 1024 * 1024), acc)
    end
  end

  describe "handle_batch/4" do
    test "uploads and publishes on success, returning messages unchanged and emitting telemetry" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :queue, :publish])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [log_event_message()]
      batch_info = %{size: 1, trigger: :size}

      result = ProducerPipeline.handle_batch(:spool, messages, batch_info, handle_batch_context())

      assert result == messages

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :put],
                      %{count: 1, bytes: bytes}, %{format: :ndjson_gz, result: :ok}}

      assert bytes > 0

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :queue, :publish],
                      %{count: 1}, %{result: :ok}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :ok, stage: nil}}
    end

    test "maps messages to failed and emits storage.put telemetry with result: :error on upload failure" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:error, :timeout} end)

      messages = [log_event_message()]
      batch_info = %{size: 1, trigger: :size}

      [result_message] =
        ProducerPipeline.handle_batch(:spool, messages, batch_info, handle_batch_context())

      assert result_message.status == {:failed, :timeout}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :put], _,
                      %{format: :ndjson_gz, result: :error}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :error, stage: :upload}}
    end

    test "publish failure still emits queue.publish telemetry with result: :error" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :queue, :publish])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      stub(QueueMod, :publish, fn _ref, _body -> {:error, :unavailable} end)

      messages = [log_event_message()]
      batch_info = %{size: 1, trigger: :size}

      ProducerPipeline.handle_batch(:spool, messages, batch_info, handle_batch_context())

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :queue, :publish],
                      %{count: 1}, %{result: :error}}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 1}, %{result: :error, stage: :notify}}
    end

    test "publish failure marks messages as failed instead of acking them, even though the file uploaded successfully" do
      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      stub(QueueMod, :publish, fn _ref, _body -> {:error, :unavailable} end)

      messages = [log_event_message()]
      batch_info = %{size: 1, trigger: :size}

      [result_message] =
        ProducerPipeline.handle_batch(:spool, messages, batch_info, handle_batch_context())

      assert result_message.status == {:failed, :unavailable}
    end

    for {format, compress, expected_tag} <- [
          {:ndjson, false, :ndjson},
          {:ndjson, true, :ndjson_gz},
          {:etf, false, :etf},
          {:etf, true, :etf_gz}
        ] do
      test "emits storage.put telemetry tagged #{expected_tag} for format=#{format} compress=#{compress}" do
        TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])

        stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
        stub(QueueMod, :publish, fn _ref, _body -> :ok end)

        messages = [log_event_message()]
        batch_info = %{size: 1, trigger: :size}
        context = handle_batch_context(format: unquote(format), compress: unquote(compress))

        ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

        assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :put], _,
                        %{format: unquote(expected_tag), result: :ok}}
      end
    end
  end

  describe "handle_batch/4 via_rule_id preservation" do
    test "ndjson output includes via_rule_id" do
      test_pid = self()

      stub(StorageMod, :put, fn _bucket, _key, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [log_event_message(%{"message" => "hello"}, 123)]
      batch_info = %{size: 1, trigger: :size}
      context = handle_batch_context(format: :ndjson, compress: false)

      ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

      assert_receive {:put, body}
      assert %{"via_rule_id" => 123} = Jason.decode!(String.trim(body))
    end

    test "etf output includes via_rule_id" do
      test_pid = self()

      stub(StorageMod, :put, fn _bucket, _key, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [log_event_message(%{"message" => "hello"}, 123)]
      batch_info = %{size: 1, trigger: :size}
      context = handle_batch_context(format: :etf, compress: false)

      ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

      assert_receive {:put, body}
      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(body)
    end
  end
end
