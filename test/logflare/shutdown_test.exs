defmodule Logflare.ShutdownTest do
  use ExUnit.Case, async: false

  import Mimic

  alias Logflare.IngressDrain
  alias Logflare.Shutdown
  alias Logflare.ShutdownDrain

  setup :set_mimic_global

  setup do
    parent = self()

    stub(ShutdownDrain, :stop_receiving, fn ->
      send(parent, :stop_receiving)
      :ok
    end)

    stub(IngressDrain, :run, fn deadline ->
      send(parent, {:quiesce, deadline})
      :drained
    end)

    stub(ShutdownDrain, :run, fn opts ->
      send(parent, {:flush, opts})
      :drained
    end)

    :ok
  end

  test "withdrawal and ingress completion precede the final flush" do
    deadline = System.monotonic_time(:millisecond) + 1_000

    assert Shutdown.run(
             deadline_ms: deadline,
             withdrawal_ms: 0,
             request_timeout_ms: 100,
             drain_timeout_ms: 500
           ) == :drained

    assert_receive :stop_receiving
    assert_receive {:quiesce, ingress_deadline}
    assert ingress_deadline <= deadline
    assert_receive {:flush, opts}
    assert opts[:deadline_ms] == deadline
    assert opts[:timeout_ms] <= 500
  end

  test "does not flush while an admitted request is still being awaited" do
    parent = self()

    stub(IngressDrain, :run, fn _deadline ->
      send(parent, {:waiting, self()})

      receive do
        :release -> :drained
      end
    end)

    task =
      Task.async(fn ->
        Shutdown.run(
          deadline_ms: System.monotonic_time(:millisecond) + 2_000,
          withdrawal_ms: 0,
          request_timeout_ms: 1_000,
          drain_timeout_ms: 500
        )
      end)

    assert_receive {:waiting, worker}
    refute_receive {:flush, _}
    send(worker, :release)
    assert Task.await(task) == :drained
    assert_receive {:flush, _}
  end

  test "an ingress timeout is never reported as a successful drain" do
    stub(IngressDrain, :run, fn _deadline -> :timeout end)

    assert Shutdown.run(
             deadline_ms: System.monotonic_time(:millisecond) + 1_000,
             withdrawal_ms: 0,
             request_timeout_ms: 10,
             drain_timeout_ms: 500
           ) == :timeout
  end

  test "without spool drain configuration, still quiesces ingress before stopping" do
    assert Shutdown.run(
             deadline_ms: System.monotonic_time(:millisecond) + 1_000,
             withdrawal_ms: 0,
             request_timeout_ms: 100,
             drain_timeout_ms: nil
           ) == :drained

    assert_receive {:quiesce, _}
    refute_receive {:flush, _}
  end

  test "withdrawal is bounded by the shared deadline" do
    started = System.monotonic_time(:millisecond)

    Shutdown.run(
      deadline_ms: started + 20,
      withdrawal_ms: 10_000,
      request_timeout_ms: 100,
      drain_timeout_ms: 500
    )

    assert System.monotonic_time(:millisecond) - started < 500
    assert_receive {:flush, opts}
    assert opts[:timeout_ms] == 0
  end
end
