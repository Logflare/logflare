defmodule Logflare.Backends.Spool.MemoryMonitorTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.IngestEventQueue
  alias Logflare.Backends.Spool.MemoryMonitor
  alias Logflare.TestUtils

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

  test "throttled?/0 is true once the configured percent thresholds are exceeded" do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)

    assert MemoryMonitor.throttled?() == true
  end

  test "throttled?/0 is false when comfortably under the configured percent thresholds" do
    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 1.0,
      spool_max_ets_percent: 1.0
    )

    start_supervised!(MemoryMonitor)
    Process.sleep(50)

    assert MemoryMonitor.throttled?() == false
  end

  test "refresh/0 emits a [:logflare, :backends, :spool, :throttled] telemetry event on every refresh" do
    TestUtils.attach_forwarder([:logflare, :backends, :spool, :throttled])

    Application.put_env(:logflare, :spool,
      spool_memory_limit_percent: 0.0,
      spool_max_ets_percent: 0.0
    )

    start_supervised!(MemoryMonitor)

    assert_receive {:telemetry_event, [:logflare, :backends, :spool, :throttled],
                    %{
                      throttled: 1,
                      total_percent: total_percent,
                      ets_percent: ets_percent,
                      consumer_throttled: consumer_throttled
                    }, %{}},
                   1000

    assert is_float(total_percent)
    assert is_float(ets_percent)
    assert consumer_throttled in [0, 1]
  end

  describe "consumer_throttled?/0" do
    setup do
      insert(:plan)
      user = insert(:user)
      source = insert(:source, user: user)
      table_key = {source.id, nil, self()}
      IngestEventQueue.upsert_tid(table_key)

      Application.put_env(:logflare, :spool,
        spool_memory_limit_percent: 1.0,
        spool_max_ets_percent: 1.0
      )

      pid = start_supervised!(MemoryMonitor)
      Process.sleep(50)

      {:ok, user: user, source: source, table_key: table_key, pid: pid}
    end

    defp force_refresh(pid) do
      send(pid, :refresh)
      :sys.get_state(pid)
    end

    test "is true once a registered source's destination buffer is full", %{
      source: source,
      table_key: table_key,
      pid: pid
    } do
      assert MemoryMonitor.consumer_throttled?() == false

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      MemoryMonitor.register_source(source.id)
      force_refresh(pid)

      assert MemoryMonitor.consumer_throttled?() == true
    end

    test "goes back to false once the registered source's buffer drains", %{
      source: source,
      table_key: table_key,
      pid: pid
    } do
      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      MemoryMonitor.register_source(source.id)
      force_refresh(pid)
      assert MemoryMonitor.consumer_throttled?() == true

      IngestEventQueue.truncate_table(table_key, :pending, 0)
      force_refresh(pid)

      assert MemoryMonitor.consumer_throttled?() == false
    end

    test "an unregistered source's backlog does not affect consumer_throttled?/0", %{
      table_key: table_key,
      pid: pid
    } do
      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      # never registered
      force_refresh(pid)

      assert MemoryMonitor.consumer_throttled?() == false
    end

    test "a source that no longer resolves does not throttle or crash the refresh cycle", %{
      pid: pid
    } do
      MemoryMonitor.register_source(999_999_999)
      force_refresh(pid)

      assert MemoryMonitor.consumer_throttled?() == false
      assert Process.alive?(pid)
    end

    test "registering the same source repeatedly is idempotent and keeps it watched", %{
      source: source,
      table_key: table_key,
      pid: pid
    } do
      MemoryMonitor.register_source(source.id)
      MemoryMonitor.register_source(source.id)
      MemoryMonitor.register_source(source.id)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(table_key, [le])
      end

      force_refresh(pid)

      assert MemoryMonitor.consumer_throttled?() == true
    end

    test "detects backlog on a backend that isn't flagged as default-ingest", %{
      user: user,
      source: source,
      pid: pid
    } do
      # This is the exact bug the earlier cached_local_pending_buffer_full?/1
      # based check missed: it only ever looks at the system default (nil)
      # queue plus backends explicitly flagged default_ingest?: true — a
      # perfectly normal, non-default-ingest backend (e.g. ClickHouse) can be
      # completely backlogged and it would report "not full" regardless.
      backend = insert(:backend, user: user, type: :clickhouse, default_ingest?: false)
      {:ok, _source} = Backends.update_source_backends(source, [backend])
      backend_table_key = {source.id, backend.id, self()}
      IngestEventQueue.upsert_tid(backend_table_key)

      for _ <- 1..(Backends.max_buffer_queue_len() + 500) do
        le = build(:log_event)
        IngestEventQueue.add_to_table(backend_table_key, [le])
      end

      MemoryMonitor.register_source(source.id)
      force_refresh(pid)

      assert MemoryMonitor.consumer_throttled?() == true
    end
  end
end
