defmodule Logflare.SigtermHandlerTest do
  use ExUnit.Case, async: false

  import ExUnit.CaptureLog
  import Mimic

  alias Logflare.Application
  alias Logflare.Backends
  alias Logflare.Backends.Spool.ConsumerDrain
  alias Logflare.Readiness
  alias Logflare.SigtermHandler

  setup :set_mimic_global

  setup do
    reset_readiness()
    on_exit(&reset_readiness/0)

    grace = Elixir.Application.get_env(:logflare, :sigterm_shutdown_grace_period_ms)

    on_exit(fn ->
      Elixir.Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, grace)
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
      stub(ConsumerDrain, :run, fn -> :drained end)
      stub(ConsumerDrain, :timeout_ms, fn -> 60_000 end)
      :ok
    end

    test "logs that SIGTERM was received" do
      stub(Backends, :spool_consumer_mode?, fn -> false end)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigterm, %{}) end)

      assert log =~ "SIGTERM received"
    end

    test "logs that a spool consumer received SIGTERM and is draining before it stops" do
      stub(Backends, :spool_consumer_mode?, fn -> true end)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigterm, %{}) end)

      assert log =~ "SIGTERM received: draining spool consumer for up to 60.0 seconds"
    end

    test "logs that a spool consumer received SIGQUIT and is draining before it stops" do
      stub(Backends, :spool_consumer_mode?, fn -> true end)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigquit, %{}) end)

      assert log =~ "SIGQUIT received: draining spool consumer for up to 60.0 seconds"
    end

    test "logs the drain result once the spool consumer is ready to stop" do
      stub(Backends, :spool_consumer_mode?, fn -> true end)
      stub(ConsumerDrain, :run, fn -> :timeout end)

      log = capture_warning(fn -> SigtermHandler.handle_event(:sigterm, %{}) end)

      assert log =~ "spool consumer drain ended with :timeout, stopping the app"
    end

    defp capture_warning(fun) do
      capture_log([level: :warning], fn ->
        fun.()
        assert_receive :proceed_with_sigterm
      end)
    end
  end

  describe "spool consumer drain" do
    setup do
      test_pid = self()

      stub(ConsumerDrain, :run, fn ->
        send(test_pid, :drain_ran)
        :drained
      end)

      :ok
    end

    test "drains the spool consumer before stopping the app" do
      put_grace_period(0)
      stub(Backends, :spool_consumer_mode?, fn -> true end)

      assert {:ok, %{}} = SigtermHandler.handle_event(:sigterm, %{})

      assert_receive :drain_ran
      assert_receive :proceed_with_sigterm
    end

    test "still waits out the grace period when the drain finishes sooner" do
      put_grace_period(400)
      stub(Backends, :spool_consumer_mode?, fn -> true end)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive :drain_ran
      refute_receive :proceed_with_sigterm, 200
      assert_receive :proceed_with_sigterm, 500
    end

    test "stops the app even if the drain crashes" do
      put_grace_period(0)
      stub(Backends, :spool_consumer_mode?, fn -> true end)
      stub(ConsumerDrain, :run, fn -> raise "boom" end)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive :proceed_with_sigterm
    end

    test "drains on SIGQUIT too" do
      put_grace_period(0)
      stub(Backends, :spool_consumer_mode?, fn -> true end)

      SigtermHandler.handle_event(:sigquit, %{})

      assert_receive :drain_ran
      assert_receive :proceed_with_sigterm
    end

    test "skips the drain outside spool consumer mode" do
      put_grace_period(0)
      stub(Backends, :spool_consumer_mode?, fn -> false end)

      SigtermHandler.handle_event(:sigterm, %{})

      assert_receive :proceed_with_sigterm
      refute_received :drain_ran
    end
  end

  defp put_grace_period(ms) do
    Elixir.Application.put_env(:logflare, :sigterm_shutdown_grace_period_ms, ms)
  end

  defp reset_readiness do
    Readiness.initialize()
    Readiness.mark_ready()
  end
end
