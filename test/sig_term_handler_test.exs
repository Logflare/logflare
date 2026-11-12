defmodule Logflare.SigtermHandlerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Readiness
  alias Logflare.Shutdown
  alias Logflare.SigtermHandler

  setup :set_mimic_global

  setup do
    keys = [
      :sigterm_shutdown_grace_period_ms,
      :shutdown_drain_timeout_ms,
      :shutdown_timeout_ms,
      :shutdown_request_timeout_ms
    ]

    previous = Map.new(keys, &{&1, Application.get_env(:logflare, &1)})
    Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, 0)
    Application.put_env(:logflare, :shutdown_drain_timeout_ms, 60_000)
    Application.put_env(:logflare, :shutdown_timeout_ms, 500)
    Application.put_env(:logflare, :shutdown_request_timeout_ms, 100)
    Readiness.initialize()
    Readiness.mark_ready()
    stub(Shutdown, :run, fn _opts -> :drained end)

    on_exit(fn ->
      for {key, value} <- previous do
        if is_nil(value),
          do: Application.delete_env(:logflare, key),
          else: Application.put_env(:logflare, key, value)
      end

      Readiness.initialize()
      Readiness.mark_ready()
    end)
  end

  test "marks readiness false immediately and cannot become ready again" do
    state = signal(:sigterm)
    refute Readiness.ready?()
    Readiness.mark_ready()
    refute Readiness.ready?()
    assert state.shutdown_started
    assert_receive :proceed_with_sigterm
  end

  test "SIGQUIT uses the same shutdown lifecycle" do
    signal(:sigquit)
    refute Readiness.ready?()
    assert_receive :proceed_with_sigterm
  end

  test "application stop still marks readiness false" do
    assert Logflare.Application.prep_stop(:state) == :state
    refute Readiness.ready?()
  end

  test "passes one fixed deadline and separate phase budgets to the coordinator" do
    test_pid = self()

    stub(Shutdown, :run, fn opts ->
      send(test_pid, {:shutdown_opts, opts})
      :drained
    end)

    started = System.monotonic_time(:millisecond)
    signal(:sigterm)
    assert_receive {:shutdown_opts, opts}
    assert opts[:deadline_ms] >= started + 500
    assert opts[:deadline_ms] <= System.monotonic_time(:millisecond) + 500
    assert opts[:withdrawal_ms] == 0
    assert opts[:request_timeout_ms] == 100
    assert opts[:drain_timeout_ms] == 60_000
    assert_receive :proceed_with_sigterm
  end

  test "repeated and mixed signals neither restart the worker nor reset its timer" do
    test_pid = self()

    stub(Shutdown, :run, fn _opts ->
      send(test_pid, {:started, self()})

      receive do
        :release -> :drained
      end
    end)

    state = signal(:sigterm)
    assert_receive {:started, worker}
    assert {:ok, ^state} = SigtermHandler.handle_event(:sigterm, state)
    assert {:ok, ^state} = SigtermHandler.handle_event(:sigquit, state)
    refute_receive {:started, _}
    send(worker, :release)
    assert_receive :proceed_with_sigterm
  end

  test "a crashing coordinator still requests application shutdown" do
    stub(Shutdown, :run, fn _opts -> raise "drain failed" end)
    signal(:sigterm)
    assert_receive :proceed_with_sigterm
  end

  test "logs shutdown completion and timeout outcomes" do
    stub(Shutdown, :run, fn _opts -> :timeout end)

    log =
      capture_log(fn ->
        signal(:sigterm)
        assert_receive :proceed_with_sigterm
      end)

    assert log =~ "shutdown budget=500ms"
    assert log =~ "shutdown ended with :timeout"
  end

  test "ignores unrelated events without starting shutdown" do
    assert {:ok, %{}} = SigtermHandler.handle_event(:other, %{})
    assert {:ok, %{}} = SigtermHandler.handle_info(:other, %{})
  end

  defp signal(signal) do
    assert {:ok, state} = SigtermHandler.handle_event(signal, %{})

    on_exit(fn ->
      Process.cancel_timer(state.timer)
      if Process.alive?(state.worker), do: Process.exit(state.worker, :kill)
    end)

    state
  end
end
