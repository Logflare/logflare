defmodule Logflare.SigtermHandlerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Application
  alias Logflare.Readiness
  alias Logflare.ShutdownDrain
  alias Logflare.SigtermHandler

  setup :set_mimic_global

  setup do
    reset_readiness()
    on_exit(&reset_readiness/0)

    grace = Elixir.Application.get_env(:logflare, :sigterm_shutdown_grace_period_ms)
    drain_timeout = Elixir.Application.get_env(:logflare, :shutdown_drain_timeout_ms)

    on_exit(fn ->
      Elixir.Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, grace)
      Elixir.Application.put_env(:logflare, :shutdown_drain_timeout_ms, drain_timeout)
    end)
  end

  test "marks the node unready immediately after SIGTERM" do
    assert {:ok, %{}} = SigtermHandler.handle_event(:sigterm, %{})
    refute Readiness.ready?()
  end

  test "marks the node unready immediately after SIGQUIT" do
    assert {:ok, %{}} = SigtermHandler.handle_event(:sigquit, %{})
    refute Readiness.ready?()
  end

  test "marks the node unready before application shutdown" do
    state = %{test: :state}

    assert Application.prep_stop(state) == state
    refute Readiness.ready?()
  end

  test "cannot become ready after shutdown begins" do
    Readiness.begin_draining()
    Readiness.mark_ready()

    refute Readiness.ready?()
  end

  describe "verification logging" do
    setup do
      put_grace_period(0)
      stub(ShutdownDrain, :run, fn _opts -> :drained end)
      :ok
    end

    test "logs that SIGTERM was received" do
      put_drain_timeout(nil)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigterm, %{}) end)

      assert log =~ "SIGTERM received: waiting for 0.0 seconds"
    end

    test "logs that SIGTERM was received and the node is draining before it stops" do
      put_drain_timeout(60_000)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigterm, %{}) end)

      assert log =~ "SIGTERM received: draining for up to 60.0 seconds before stopping"
    end

    test "logs that SIGQUIT was received and the node is draining before it stops" do
      put_drain_timeout(60_000)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigquit, %{}) end)

      assert log =~ "SIGQUIT received: draining for up to 60.0 seconds before stopping"
    end

    test "logs the drain result once the node is ready to stop" do
      put_drain_timeout(60_000)
      stub(ShutdownDrain, :run, fn _opts -> :timeout end)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigterm, %{}) end)

      assert log =~ "shutdown drain ended with :timeout, stopping the app"
    end

    defp capture_warning(fun) do
      capture_log([level: :warning], fn ->
        fun.()
        assert_receive :proceed_with_sigterm
      end)
    end
  end

  describe "shutdown drain" do
    setup do
      test_pid = self()

      stub(ShutdownDrain, :run, fn opts ->
        send(test_pid, {:drain_ran, opts})
        :drained
      end)

      :ok
    end

    test "drains every node before stopping the app when a drain timeout is configured" do
      put_grace_period(0)
      put_drain_timeout(60_000)

      assert {:ok, %{}} = SigtermHandler.handle_event(:sigterm, %{})

      assert_receive {:drain_ran, opts}
      assert opts[:timeout_ms] == 60_000
      assert_receive :proceed_with_sigterm
    end

    test "has the drain wait out the grace period first, so load balancers stop sending" do
      put_grace_period(15_000)
      put_drain_timeout(60_000)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive {:drain_ran, opts}
      assert opts[:min_wait_ms] == 15_000
    end

    test "never drains for less than the grace period" do
      put_grace_period(15_000)
      put_drain_timeout(5_000)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive {:drain_ran, opts}
      assert opts[:timeout_ms] == 15_000
    end

    test "stops the app even if the drain crashes" do
      put_grace_period(0)
      put_drain_timeout(60_000)
      stub(ShutdownDrain, :run, fn _opts -> raise "boom" end)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive :proceed_with_sigterm
    end

    test "drains on SIGQUIT too" do
      put_grace_period(0)
      put_drain_timeout(60_000)

      SigtermHandler.handle_event(:sigquit, %{})

      assert_receive {:drain_ran, _opts}
      assert_receive :proceed_with_sigterm
    end

    test "keeps the plain grace period when no drain timeout is configured" do
      put_grace_period(0)
      put_drain_timeout(nil)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive :proceed_with_sigterm
      refute_received {:drain_ran, _opts}
    end
  end

  defp put_drain_timeout(ms) do
    Elixir.Application.put_env(:logflare, :shutdown_drain_timeout_ms, ms)
  end

  defp put_grace_period(ms) do
    Elixir.Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, ms)
  end

  defp reset_readiness do
    Readiness.initialize()
    Readiness.mark_ready()
  end
end
