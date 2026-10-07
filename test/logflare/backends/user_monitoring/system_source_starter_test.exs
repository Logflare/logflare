defmodule Logflare.Backends.UserMonitoring.SystemSourceStarterTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.UserMonitoring.SystemSourceStarter
  alias Logflare.SystemMetrics.AllLogsLogged

  setup :set_mimic_global

  setup do
    Application.put_env(:logflare, :system_source_starter_retry_interval, 50)

    on_exit(fn ->
      Application.delete_env(:logflare, :system_source_starter_retry_interval)
      Application.delete_env(:logflare, :system_source_starter_max_buffer)

      :sys.replace_state(SystemSourceStarter, fn _state -> SystemSourceStarter.empty_state() end)
    end)

    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)
    [source: source]
  end

  test "runs one start while a start is in flight, then ingests every held event", %{
    source: source
  } do
    test_pid = self()
    calls = :counters.new(1, [])

    stub(Backends, :ensure_source_sup_started, fn received ->
      :counters.add(calls, 1, 1)

      if :counters.get(calls, 1) == 1 do
        send(test_pid, {:start, self()})

        receive do
          :release -> :ok
        end
      end

      call_original(Backends, :ensure_source_sup_started, [received])
    end)

    for i <- 1..50, do: SystemSourceStarter.buffer(source.id, [%{"message" => "event #{i}"}])

    assert_receive {:start, task_pid}
    TestUtils.retry_assert(fn -> assert {50, _held} = buffers()[source.id] end)
    assert :counters.get(calls, 1) == 1

    send(task_pid, :release)

    TestUtils.retry_assert(fn ->
      assert buffers() == %{}
      assert length(Backends.list_recent_logs_local(source)) == 50
    end)
  end

  test "keeps the held events after a failed start and retries the start", %{source: source} do
    calls = :counters.new(1, [])

    stub(Backends, :ensure_source_sup_started, fn received ->
      :counters.add(calls, 1, 1)

      if :counters.get(calls, 1) == 1 do
        {:error, :start_timeout}
      else
        call_original(Backends, :ensure_source_sup_started, [received])
      end
    end)

    SystemSourceStarter.buffer(source.id, [%{"message" => "after retry"}])

    TestUtils.retry_assert(fn ->
      assert Backends.source_sup_started?(source)
      assert [_event] = Backends.list_recent_logs_local(source)
    end)
  end

  test "puts the events back and retries when the ingest after the start fails", %{
    source: source
  } do
    calls = :counters.new(1, [])

    stub(Backends, :ensure_source_sup_started, fn received ->
      :counters.add(calls, 1, 1)

      if :counters.get(calls, 1) == 2 do
        {:error, :start_timeout}
      else
        call_original(Backends, :ensure_source_sup_started, [received])
      end
    end)

    SystemSourceStarter.buffer(source.id, [%{"message" => "after failed ingest"}])

    TestUtils.retry_assert(fn ->
      assert [_event] = Backends.list_recent_logs_local(source)
      assert buffers() == %{}
    end)

    assert :counters.get(calls, 1) >= 4
  end

  test "keeps the held events after a start that raises", %{source: source} do
    starter = Process.whereis(SystemSourceStarter)
    calls = :counters.new(1, [])

    stub(Backends, :ensure_source_sup_started, fn received ->
      :counters.add(calls, 1, 1)

      if :counters.get(calls, 1) == 1 do
        raise "boom"
      else
        call_original(Backends, :ensure_source_sup_started, [received])
      end
    end)

    ExUnit.CaptureLog.capture_log(fn ->
      SystemSourceStarter.buffer(source.id, [%{"message" => "after raise"}])

      TestUtils.retry_assert(fn ->
        assert [_event] = Backends.list_recent_logs_local(source)
      end)
    end)

    assert Process.whereis(SystemSourceStarter) == starter
  end

  test "drops the held events when the source does not exist" do
    attach_dropped()
    missing_id = 999_999_999

    SystemSourceStarter.buffer(missing_id, [%{"message" => "a"}, %{"message" => "b"}])

    assert_receive {:dropped, %{count: 2}, %{source_id: ^missing_id, reason: :not_found}}
    TestUtils.retry_assert(fn -> assert buffers() == %{} end)
  end

  test "drops the events over the buffer cap", %{source: source} do
    Application.put_env(:logflare, :system_source_starter_max_buffer, 3)
    attach_dropped()
    test_pid = self()
    calls = :counters.new(1, [])

    stub(Backends, :ensure_source_sup_started, fn received ->
      :counters.add(calls, 1, 1)

      if :counters.get(calls, 1) == 1 do
        send(test_pid, {:start, self()})

        receive do
          :release -> :ok
        end
      end

      call_original(Backends, :ensure_source_sup_started, [received])
    end)

    events = for i <- 1..5, do: %{"message" => "event #{i}"}
    SystemSourceStarter.buffer(source.id, events)

    source_id = source.id
    assert_receive {:dropped, %{count: 2}, %{source_id: ^source_id, reason: :buffer_full}}
    assert_receive {:start, task_pid}
    send(task_pid, :release)

    TestUtils.retry_assert(fn ->
      assert length(Backends.list_recent_logs_local(source)) == 3
    end)
  end

  defp buffers, do: :sys.get_state(SystemSourceStarter).buffers

  defp attach_dropped do
    test_pid = self()
    ref = make_ref()
    event = [:logflare, :user_monitoring, :system_source_starter, :dropped]

    :telemetry.attach(
      ref,
      event,
      fn ^event, measurements, metadata, _ ->
        send(test_pid, {:dropped, measurements, metadata})
      end,
      nil
    )

    on_exit(fn -> :telemetry.detach(ref) end)
  end
end
