defmodule Logflare.Backends.Spool.IngestMetricsDoubleCountTest do
  use Logflare.DataCase, async: false

  import Mimic

  alias Broadway.Message
  alias Logflare.Backends
  alias Logflare.Backends.SourceSup
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.DurableBuffer.Supervisor, as: SpoolDurableBufferSup
  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Queue.PubSub, as: SpoolQueueMod
  alias Logflare.Backends.Spool.SpoolAck
  alias Logflare.Backends.Spool.Storage.GCS, as: SpoolStorageMod
  alias Logflare.Backends.UserMonitoring
  alias Logflare.Sources
  alias Logflare.SystemMetrics.AllLogsLogged

  setup :set_mimic_global

  setup do
    insert(:plan)
    user = insert(:user, system_monitoring: true)
    source_a = insert(:source, user: user)
    source_b = insert(:source, user: user)
    metrics_source = insert(:source, user: user, system_source_type: :metrics)

    start_supervised!(AllLogsLogged)
    Enum.each(UserMonitoring.get_otel_exporter(), &start_supervised!/1)
    start_supervised!({SourceSup, metrics_source}, id: :metrics_source)
    start_supervised!({SourceSup, source_a}, id: :source_a)
    start_supervised!({SourceSup, source_b}, id: :source_b)

    test_pid = self()

    stub(GoogleApi.BigQuery.V2.Api.Tabledata, :bigquery_tabledata_insert_all, fn _conn,
                                                                                 _project,
                                                                                 _dataset,
                                                                                 table,
                                                                                 opts ->
      rows = opts[:body].rows
      send(test_pid, {:bq_insert, table, length(rows)})
      {:ok, %GoogleApi.BigQuery.V2.Model.TableDataInsertAllResponse{insertErrors: nil}}
    end)

    stub(SpoolQueueMod, :ack, fn _ref, handle ->
      send(test_pid, {:queue_acked, handle})
      :ok
    end)

    handler_id = "ingest-metrics-double-count-#{System.unique_integer([:positive])}"

    :telemetry.attach(
      handler_id,
      [:logflare, :backends, :ingest],
      fn _event, _measurements, %{"source_uuid" => uuid, "system_source" => false}, _ ->
        send(test_pid, {:ingested, uuid})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler_id) end)

    prev_spool_config = Application.get_env(:logflare, :spool)

    on_exit(fn ->
      if prev_spool_config,
        do: Application.put_env(:logflare, :spool, prev_spool_config),
        else: Application.delete_env(:logflare, :spool)
    end)

    [source_a: source_a, source_b: source_b, metrics_source: metrics_source]
  end

  describe "spool round trip, happy path" do
    setup :start_spool_buffer

    test "each event is counted once in system.metrics", ctx do
      %{source_a: source_a, metrics_source: metrics_source} = ctx
      put_spool_mode(:producer)

      assert {:ok, 3} = Backends.ingest_logs(params(3), source_a, nil, true)

      records = assert_spooled_records()
      assert length(records) == 3

      refute_received {:ingested, _}
      refute_received {:bq_insert, _, _}

      put_spool_mode(:disable)
      handle = register_handle()

      [message] =
        ConsumerPipeline.handle_batch(:default, [consumer_message(records, handle)], %{}, %{})

      ack_consumer([message])

      assert_ingested(source_a, 3)
      assert_receive {:queue_acked, ^handle}, 5_000
      assert_metrics_total(metrics_source, source_a, 3)
    end
  end

  describe "consumer redelivery after a partial dispatch failure" do
    test "a segment that spans two sources re-ingests and re-counts the healthy source's events",
         ctx do
      %{source_a: source_a, source_b: source_b, metrics_source: metrics_source} = ctx
      source_b_id = source_b.id

      records =
        Enum.map(1..3, &record(source_a, &1)) ++ Enum.map(4..5, &record(source_b, &1))

      stub(Sources.Cache, :get_by, fn
        [id: ^source_b_id] -> raise DBConnection.ConnectionError, "simulated db blip"
        args -> call_original(Sources.Cache, :get_by, [args])
      end)

      handle = register_handle()

      [message] =
        ConsumerPipeline.handle_batch(:default, [consumer_message(records, handle)], %{}, %{})

      assert {:failed, :dispatch_error} = message.status
      ack_consumer([message])

      assert_ingested(source_a, 3)
      refute_received {:queue_acked, ^handle}

      stub(Sources.Cache, :get_by, fn args -> call_original(Sources.Cache, :get_by, [args]) end)

      redelivered = register_handle()

      [message] =
        ConsumerPipeline.handle_batch(
          :default,
          [consumer_message(records, redelivered)],
          %{},
          %{}
        )

      assert message.status == :ok
      ack_consumer([message])

      assert_ingested(source_a, 3)
      assert_ingested(source_b, 2)
      assert_receive {:queue_acked, ^redelivered}, 5_000

      assert_metrics_total(metrics_source, source_a, 6)
      assert_metrics_total(metrics_source, source_b, 2)
    end
  end

  describe ":both mode append timeout" do
    setup :start_spool_buffer

    @tag timeout: 60_000
    test "falls back to direct dispatch while the timed-out append still lands in the spool",
         ctx do
      %{source_a: source_a, metrics_source: metrics_source} = ctx
      put_spool_mode(:both)
      spooling_source_a = %{source_a | enable_spooling: true}

      [partition] = SpoolDurableBufferSup.partitions()
      :ok = :sys.suspend(partition)

      task = Task.async(fn -> Backends.ingest_logs(params(2), spooling_source_a, nil, true) end)
      assert {:ok, 2} = Task.await(task, 30_000)

      assert_ingested(source_a, 2)

      :ok = :sys.resume(partition)
      records = Enum.filter(assert_spooled_records(), &(&1.source_id == source_a.id))
      assert length(records) == 2

      put_spool_mode(:disable)
      handle = register_handle()

      [message] =
        ConsumerPipeline.handle_batch(:default, [consumer_message(records, handle)], %{}, %{})

      ack_consumer([message])

      assert_ingested(source_a, 2)
      assert_metrics_total(metrics_source, source_a, 4)
    end
  end

  defp start_spool_buffer(_ctx) do
    test_pid = self()

    wal_dir =
      Path.join(System.tmp_dir!(), "ingest_metrics_spool_#{System.unique_integer([:positive])}")

    File.mkdir_p!(wal_dir)
    on_exit(fn -> File.rm_rf!(wal_dir) end)

    stub(SpoolStorageMod, :put, fn _bucket, key, body, _opts ->
      send(test_pid, {:spooled, key, body})
      {:ok, key}
    end)

    stub(SpoolQueueMod, :publish, fn _ref, _body -> :ok end)

    Application.put_env(:logflare, :spool,
      mode: :disable,
      buffer: :wal,
      partitions: 1,
      flush_delay_ms: 10,
      wal_max_rotation_interval_ms: 10,
      max_commit_attempts: 1,
      retry_delay_ms: 1,
      bucket: "test-bucket",
      storage_mod: SpoolStorageMod,
      queue_mod: SpoolQueueMod,
      wal_dir: wal_dir
    )

    start_supervised!(SpoolDurableBufferSup)
    :ok
  end

  defp put_spool_mode(mode, extra \\ []) do
    config = Application.get_env(:logflare, :spool, [])

    Application.put_env(
      :logflare,
      :spool,
      config |> Keyword.merge(extra) |> Keyword.put(:mode, mode)
    )
  end

  defp params(n) do
    for i <- 1..n,
        do: %{"message" => "event #{i}", "timestamp" => System.system_time(:microsecond)}
  end

  defp record(source, i) do
    %{
      id: Ecto.UUID.generate(),
      source_id: source.id,
      body: %{"event_message" => "event #{i}", "timestamp" => System.system_time(:microsecond)},
      event_type: :log,
      ingested_at: System.system_time(:microsecond),
      via_rule_id: nil
    }
  end

  defp assert_spooled_records do
    assert_receive {:spooled, key, body}, 30_000

    decompressed =
      if String.ends_with?(key, ".zst"), do: :ezstd.decompress(body), else: body

    {segments, _valid, _rest} = Encoder.decode_segments(decompressed)
    Enum.flat_map(segments, &:erlang.binary_to_term/1)
  end

  defp register_handle do
    handle = "handle-#{System.unique_integer([:positive])}"
    SpoolAck.register(handle, SpoolQueueMod, "test-queue")
    SpoolAck.bump(handle, 1)
    handle
  end

  defp consumer_message(records, handle) do
    %Message{
      data: records,
      acknowledger: {ConsumerPipeline, :noop, %{in_flight_ref: nil, bytes: 0, handle: handle}}
    }
  end

  defp ack_consumer(messages) do
    {successful, failed} = Enum.split_with(messages, &(&1.status == :ok))
    ConsumerPipeline.ack(:noop, successful, failed)
  end

  defp assert_ingested(source, expected) do
    uuid = to_string(source.token)

    for _ <- List.duplicate(nil, expected) do
      assert_receive {:ingested, ^uuid}, 15_000
    end

    refute_receive {:ingested, ^uuid}, 1_500
  end

  defp assert_metrics_total(metrics_source, source, expected) do
    uuid = to_string(source.token)

    TestUtils.retry_assert(fn ->
      total =
        metrics_source
        |> Backends.list_recent_logs_local()
        |> Enum.filter(fn le ->
          le.body["event_message"] == "logflare.backends.ingest.ingested_count" and
            le.body["attributes"]["source_uuid"] == uuid
        end)
        |> Enum.map(& &1.body["value"])
        |> Enum.sum()

      assert total == expected
    end)
  end
end
