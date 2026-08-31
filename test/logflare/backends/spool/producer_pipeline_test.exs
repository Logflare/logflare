defmodule Logflare.Backends.Spool.ProducerPipelineTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Broadway.Message
  alias Logflare.Backends.Spool.EventQueue
  alias Logflare.Backends.Spool.EventQueue.Chunk
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.Backends.Spool.ProducerPipeline
  alias Logflare.Backends.Spool.Queue.PubSub, as: QueueMod
  alias Logflare.Backends.Spool.Storage.GCS, as: StorageMod
  alias Logflare.LogEvent
  alias Logflare.TestUtils

  setup :set_mimic_global

  @max_spool_file_size 7 * 1024 * 1024
  @early_flush_file_size 3 * 1024 * 1024

  setup do
    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      if prev_spool_config do
        Application.put_env(:logflare, :spool, prev_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    EventQueue.init_table()
    drain_event_queue()

    :ok
  end

  defp drain_event_queue do
    case EventQueue.pop(1_000) do
      [] ->
        :ok

      chunks ->
        Enum.each(chunks, &EventQueue.delete_events(&1.ref))
        drain_event_queue()
    end
  end

  defp log_event(body \\ %{"message" => "hello"}, via_rule_id \\ nil) do
    %LogEvent{
      id: Ecto.UUID.generate(),
      source_id: 1,
      body: body,
      event_type: :log,
      ingested_at: DateTime.utc_now(),
      valid: true,
      drop: false,
      via_rule_id: via_rule_id
    }
  end

  # A synthetic message carrying a chunk pointer with an explicit `byte_size` and
  # `event_count` — used by the splitter tests below, which only read those two
  # fields directly off the pointer (see Chunk's moduledoc) and never resolve the
  # body, so there's no need to push anything into EventQueue here.
  defp sized_message(byte_size, event_count \\ 1) do
    %{
      data: %Chunk{
        ref: make_ref(),
        caller_pid: nil,
        byte_size: byte_size,
        event_count: event_count,
        retries: 0
      }
    }
  end

  # Pushes `events` into EventQueue for real, so handle_batch/4's
  # EventQueue.get_events/1 resolves them exactly as it would for a real chunk —
  # then wraps the returned ref in our own pointer struct so tests can still
  # override caller_pid/retries/in_flight_ref for ack/retry scenarios. The push
  # also inserts a pointer row into the pending table that we don't want (we're
  # building our own pointer below, never popped from there) — immediately pop
  # and discard it so EventQueue.count()/pop/1 assertions only ever see pointers
  # tests or ack/3 explicitly put there (e.g. via requeue/1).
  defp chunk_message(events, opts \\ []) do
    {:ok, ref} = EventQueue.push(events, Keyword.get(opts, :caller_pid))
    EventQueue.pop(1)

    chunk = %Chunk{
      ref: ref,
      caller_pid: Keyword.get(opts, :caller_pid),
      byte_size: Keyword.get(opts, :byte_size, 0),
      event_count: length(events),
      retries: Keyword.get(opts, :retries, 0)
    }

    %Message{
      data: chunk,
      acknowledger:
        {ProducerPipeline, :no_ack_ref, %{in_flight_ref: Keyword.get(opts, :in_flight_ref)}}
    }
  end

  defp handle_batch_context(overrides \\ []) do
    [
      bucket: "test-bucket",
      partitions: 1,
      compress: true,
      format: :ndjson,
      compression_algorithm: :gzip,
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
    :sys.get_state(MemoryMonitor)
  end

  defp throttled! do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)
    :sys.get_state(MemoryMonitor)
  end

  describe "spool_batch_size_splitter/0 when not throttled" do
    test "continues accumulating under the normal 7MB budget" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, {_count, remaining}} = reducer.(sized_message(2 * 1024 * 1024), initial)
      assert remaining == @max_spool_file_size - 2 * 1024 * 1024
    end

    test "emits once accumulated size would exceed the normal 7MB budget" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, acc} = reducer.(sized_message(4 * 1024 * 1024), initial)
      assert {:emit, {_count, :pending}} = reducer.(sized_message(4 * 1024 * 1024), acc)
    end

    test "a single chunk larger than the whole budget is still admitted, and closes the batch" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:emit, {_count, :pending}} = reducer.(sized_message(64 * 1024 * 1024), initial)
    end

    test "emits once accumulated event count would exceed the 500_000 cap" do
      not_throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:emit, {_count, :pending}} = reducer.(sized_message(1, 500_001), initial)
    end
  end

  describe "spool_batch_size_splitter/0 when throttled" do
    test "emits at the smaller early-flush budget instead of the normal 7MB one" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      # 5MB alone wouldn't trip the normal 7MB budget, but does trip the 3MB one.
      assert {:emit, {_count, :pending}} = reducer.(sized_message(5 * 1024 * 1024), initial)
    end

    test "continuing under the early-flush budget still tracks the correct remaining bytes" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      assert {:cont, {_count, remaining}} = reducer.(sized_message(1 * 1024 * 1024), initial)
      assert remaining == @early_flush_file_size - 1 * 1024 * 1024
    end

    test "the budget decided for a batch stays locked in even if throttling clears mid-batch" do
      throttled!()
      {initial, reducer} = ProducerPipeline.spool_batch_size_splitter()

      # First message decides the budget for this whole batch: 3MB (throttled).
      assert {:cont, acc} = reducer.(sized_message(1 * 1024 * 1024), initial)

      # Flip to "not throttled" and wait for MemoryMonitor to genuinely refresh —
      # if the reducer re-checked per message, this would flip its behavior.
      Application.put_env(:logflare, :spool,
        spool_memory_limit_percent: 1.0,
        spool_max_ets_percent: 1.0
      )

      TestUtils.send_and_wait_for_handling(MemoryMonitor, :refresh)
      assert MemoryMonitor.throttled?() == false

      # 1MB + 2.5MB = 3.5MB, over the locked-in 3MB budget (would NOT emit under
      # a freshly-rechecked 7MB budget) — proves the decision wasn't re-evaluated.
      assert {:emit, {_count, :pending}} =
               reducer.(sized_message(round(2.5 * 1024 * 1024)), acc)
    end
  end

  describe "handle_batch/4" do
    test "uploads and publishes on success, returning messages unchanged and emitting telemetry" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :queue, :publish])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [chunk_message([log_event()])]
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

    test "flattens multiple chunks (from different callers) into one upload" do
      test_pid = self()

      stub(StorageMod, :put, fn _bucket, _key, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [
        chunk_message([log_event(%{"message" => "one"})]),
        chunk_message([log_event(%{"message" => "two"}), log_event(%{"message" => "three"})])
      ]

      batch_info = %{size: 3, trigger: :size}
      context = handle_batch_context(compress: false)

      ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

      assert_receive {:put, body}
      lines = body |> String.trim() |> String.split("\n")
      assert length(lines) == 3
    end

    test "batch telemetry counts events, not messages/chunks" do
      TestUtils.attach_forwarder([:logflare, :backends, :pipeline, :handle_batch])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      # 2 messages (chunks) carrying 3 events total — batch_info.size is a *message*
      # count from Broadway and deliberately doesn't match, to prove the telemetry
      # below counts events, not messages.
      messages = [
        chunk_message([log_event()]),
        chunk_message([log_event(), log_event()])
      ]

      batch_info = %{size: 2, trigger: :size}

      ProducerPipeline.handle_batch(:spool, messages, batch_info, handle_batch_context())

      assert_receive {:telemetry_event, [:logflare, :backends, :pipeline, :handle_batch],
                      %{batch_size: 3}, _}

      assert_receive {:telemetry_event, [:logflare, :backends, :spool, :producer, :batch],
                      %{count: 3}, %{result: :ok, stage: nil}}
    end

    test "maps messages to failed and emits storage.put telemetry with result: :error on upload failure" do
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])
      TestUtils.attach_forwarder([:logflare, :backends, :spool, :producer, :batch])

      stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:error, :timeout} end)

      messages = [chunk_message([log_event()])]
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

      messages = [chunk_message([log_event()])]
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

      messages = [chunk_message([log_event()])]
      batch_info = %{size: 1, trigger: :size}

      [result_message] =
        ProducerPipeline.handle_batch(:spool, messages, batch_info, handle_batch_context())

      assert result_message.status == {:failed, :unavailable}
    end

    for {format, compress, algorithm, expected_tag} <- [
          {:ndjson, false, :gzip, :ndjson},
          {:ndjson, true, :gzip, :ndjson_gz},
          {:ndjson, true, :zstd, :ndjson_zstd},
          {:etf, false, :gzip, :etf},
          {:etf, true, :gzip, :etf_gz},
          {:etf, true, :zstd, :etf_zstd}
        ] do
      test "emits storage.put telemetry tagged #{expected_tag} for format=#{format} compress=#{compress} algorithm=#{algorithm}" do
        TestUtils.attach_forwarder([:logflare, :backends, :spool, :storage, :put])

        stub(StorageMod, :put, fn _bucket, _key, _body, _opts -> {:ok, %{}} end)
        stub(QueueMod, :publish, fn _ref, _body -> :ok end)

        messages = [chunk_message([log_event()])]
        batch_info = %{size: 1, trigger: :size}

        context =
          handle_batch_context(
            format: unquote(format),
            compress: unquote(compress),
            compression_algorithm: unquote(algorithm)
          )

        ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

        assert_receive {:telemetry_event, [:logflare, :backends, :spool, :storage, :put], _,
                        %{format: unquote(expected_tag), result: :ok}}
      end
    end
  end

  describe "handle_batch/4 zstd compression" do
    test "ndjson+zstd body round-trips to the original JSON lines" do
      test_pid = self()

      stub(StorageMod, :put, fn _bucket, _key, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [
        chunk_message([log_event(%{"message" => "one"}), log_event(%{"message" => "two"})])
      ]

      batch_info = %{size: 1, trigger: :size}

      context =
        handle_batch_context(format: :ndjson, compress: true, compression_algorithm: :zstd)

      ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

      assert_receive {:put, body}

      lines =
        body
        |> :ezstd.decompress()
        |> String.trim()
        |> String.split("\n")
        |> Enum.map(&Jason.decode!/1)

      assert [%{"body" => %{"message" => "one"}}, %{"body" => %{"message" => "two"}}] = lines
    end

    test "etf+zstd body round-trips to the original records" do
      test_pid = self()

      stub(StorageMod, :put, fn _bucket, _key, body, _opts ->
        send(test_pid, {:put, body})
        {:ok, %{}}
      end)

      stub(QueueMod, :publish, fn _ref, _body -> :ok end)

      messages = [chunk_message([log_event(%{"message" => "hello"}, 123)])]
      batch_info = %{size: 1, trigger: :size}
      context = handle_batch_context(format: :etf, compress: true, compression_algorithm: :zstd)

      ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

      assert_receive {:put, body}
      assert [%{via_rule_id: 123}] = body |> :ezstd.decompress() |> :erlang.binary_to_term()
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

      messages = [chunk_message([log_event(%{"message" => "hello"}, 123)])]
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

      messages = [chunk_message([log_event(%{"message" => "hello"}, 123)])]
      batch_info = %{size: 1, trigger: :size}
      context = handle_batch_context(format: :etf, compress: false)

      ProducerPipeline.handle_batch(:spool, messages, batch_info, context)

      assert_receive {:put, body}
      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(body)
    end
  end

  describe "ack/3" do
    setup do
      EventQueue.init_table()
      drain()
      :ok
    end

    defp drain do
      case EventQueue.pop(1_000) do
        [] -> :ok
        _ -> drain()
      end
    end

    test "replies :ok to a successful chunk's caller" do
      message = chunk_message([log_event()], caller_pid: self())
      ref = message.data.ref

      assert :ok = ProducerPipeline.ack(:no_ack_ref, [message], [])
      assert_receive {^ref, :ok}
    end

    test "does not reply for a fire-and-forget chunk (caller_pid: nil)" do
      message = chunk_message([log_event()])

      assert :ok = ProducerPipeline.ack(:no_ack_ref, [message], [])
      refute_receive {_ref, _result}
    end

    test "with default max_retries (0), a failed chunk replies :error immediately and is not requeued" do
      message =
        [log_event()]
        |> chunk_message(caller_pid: self())
        |> Message.failed(:boom)

      ref = message.data.ref

      assert :ok = ProducerPipeline.ack(:no_ack_ref, [], [message])
      assert_receive {^ref, {:error, :boom}}
      assert EventQueue.count() == 0
    end

    test "with max_retries > 0, a failed chunk under its retry budget is requeued instead of replied to" do
      Application.put_env(:logflare, :spool, max_retries: 1)

      message =
        [log_event()]
        |> chunk_message(caller_pid: self(), retries: 0)
        |> Message.failed(:boom)

      ref = message.data.ref

      assert :ok = ProducerPipeline.ack(:no_ack_ref, [], [message])
      refute_receive {^ref, _result}

      assert [requeued] = EventQueue.pop(1)
      assert requeued.ref == ref
      assert requeued.retries == 1
    end

    test "with max_retries > 0, a chunk that already exhausted its retries is replied to, not requeued again" do
      Application.put_env(:logflare, :spool, max_retries: 1)

      message =
        [log_event()]
        |> chunk_message(caller_pid: self(), retries: 1)
        |> Message.failed(:boom)

      ref = message.data.ref

      assert :ok = ProducerPipeline.ack(:no_ack_ref, [], [message])
      assert_receive {^ref, {:error, :boom}}
      assert EventQueue.count() == 0
    end

    test "decrements the in_flight atomic by total event count across mixed chunks" do
      ref = :atomics.new(1, signed: true)
      :atomics.put(ref, 1, 3)

      ok_message =
        chunk_message([log_event()], in_flight_ref: ref)

      failed_message =
        [log_event(), log_event()]
        |> chunk_message(caller_pid: self(), in_flight_ref: ref)
        |> Message.failed(:boom)

      assert :ok = ProducerPipeline.ack(:no_ack_ref, [ok_message], [failed_message])
      assert :atomics.get(ref, 1) == 0
    end
  end
end
