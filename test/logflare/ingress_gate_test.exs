defmodule Logflare.IngressGateTest do
  use ExUnit.Case, async: false

  alias Logflare.IngressGate

  setup do
    :ets.delete_all_objects(IngressGate)
    :ets.insert(IngressGate, {:admission, :open})

    on_exit(fn ->
      :ets.delete_all_objects(IngressGate)
      :ets.insert(IngressGate, {:admission, :open})
    end)
  end

  test "closing rejects new work while preserving already admitted leases" do
    assert {:ok, token} = IngressGate.enter()
    assert :ok = IngressGate.close()
    assert :closed = IngressGate.enter()
    assert IngressGate.active_count() == 1
    assert :timeout = IngressGate.await_empty(System.monotonic_time(:millisecond))
    IngressGate.leave(token)
    assert :drained = IngressGate.await_empty(System.monotonic_time(:millisecond))
  end

  test "dead request processes cannot strand the drain" do
    parent = self()

    pid =
      spawn(fn ->
        {:ok, token} = IngressGate.enter()
        send(parent, {:entered, token})

        receive do
          :exit -> :ok
        end
      end)

    assert_receive {:entered, _}
    monitor = Process.monitor(pid)
    send(pid, :exit)
    assert_receive {:DOWN, ^monitor, :process, ^pid, :normal}
    IngressGate.close()
    assert IngressGate.active_count() == 0
  end

  test "admission stays closed under concurrent arrivals" do
    IngressGate.close()
    tasks = for _ <- 1..100, do: Task.async(fn -> IngressGate.enter() end)
    assert Enum.all?(Task.await_many(tasks), &(&1 == :closed))
    assert IngressGate.active_count() == 0
  end

  test "HTTP admission lasts until Bandit's response-completion event" do
    span = make_ref()
    metadata = %{telemetry_span_context: span}
    IngressGate.handle_event([:bandit, :request, :start], %{}, metadata, nil)
    IngressGate.close()
    assert IngressGate.http_admitted?()
    assert IngressGate.active_count() == 1
    IngressGate.handle_event([:bandit, :request, :stop], %{}, metadata, nil)
    refute IngressGate.http_admitted?()
    assert IngressGate.active_count() == 0
  end

  test "HTTP exceptions release the lease" do
    metadata = %{telemetry_span_context: make_ref()}
    IngressGate.handle_event([:bandit, :request, :start], %{}, metadata, nil)
    IngressGate.handle_event([:bandit, :request, :exception], %{}, metadata, nil)
    assert IngressGate.active_count() == 0
  end
end
