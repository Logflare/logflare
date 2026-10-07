defmodule Logflare.Backends.UserMonitoring.SystemSourceStarterTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.UserMonitoring.SystemSourceStarter

  setup :set_mimic_global

  setup do
    on_exit(fn ->
      :sys.replace_state(SystemSourceStarter, fn _state -> SystemSourceStarter.empty_state() end)
    end)

    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)
    [source: source]
  end

  test "runs one start per source while a start is in flight", %{source: source} do
    test_pid = self()
    source_id = source.id

    stub(Backends, :ensure_source_sup_started, fn received ->
      send(test_pid, {:start, received.id, self()})

      receive do
        :release -> :ok
      end
    end)

    for _ <- 1..100, do: SystemSourceStarter.request_start(source_id)

    assert_receive {:start, ^source_id, task_pid}
    refute_receive {:start, ^source_id, _pid}, 200

    send(task_pid, :release)
    TestUtils.retry_assert(fn -> assert in_flight() == %{} end)

    SystemSourceStarter.request_start(source_id)
    assert_receive {:start, ^source_id, next_task_pid}
    send(next_task_pid, :release)
    TestUtils.retry_assert(fn -> assert in_flight() == %{} end)
  end

  test "starts a source whose cache entry holds nil", %{source: source} do
    {:ok, true} = Cachex.put(Logflare.Sources.Cache, {:get_by, [[id: source.id]]}, {:cached, nil})
    on_exit(fn -> Backends.stop_source_sup(source) end)

    SystemSourceStarter.request_start(source.id)

    TestUtils.retry_assert(fn -> assert Backends.source_sup_started?(source) end)
  end

  test "survives a start that raises", %{source: source} do
    starter = Process.whereis(SystemSourceStarter)
    stub(Backends, :ensure_source_sup_started, fn _source -> raise "boom" end)

    ExUnit.CaptureLog.capture_log(fn ->
      SystemSourceStarter.request_start(source.id)
      TestUtils.retry_assert(fn -> assert in_flight() == %{} end)
    end)

    assert Process.whereis(SystemSourceStarter) == starter
    assert Process.alive?(starter)
  end

  defp in_flight, do: :sys.get_state(SystemSourceStarter).in_flight
end
