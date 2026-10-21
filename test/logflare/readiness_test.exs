defmodule Logflare.ReadinessTest do
  use ExUnit.Case, async: false

  alias Logflare.Readiness

  setup do
    Readiness.initialize()

    on_exit(fn ->
      Readiness.initialize()
      Readiness.mark_ready()
    end)
  end

  test "initialization resets the lifecycle and startup latch" do
    Readiness.mark_ready()
    Readiness.mark_startup_checks_passed()
    Readiness.begin_draining()

    assert Readiness.startup_checks_passed?()
    refute Readiness.ready?()

    Readiness.initialize()

    refute Readiness.startup_checks_passed?()
    refute Readiness.ready?()

    Readiness.mark_ready()

    assert Readiness.ready?()
    refute Readiness.startup_checks_passed?()
  end

  test "passing startup checks cannot undo draining" do
    Readiness.mark_ready()
    Readiness.begin_draining()
    Readiness.mark_startup_checks_passed()
    Readiness.mark_ready()

    assert Readiness.startup_checks_passed?()
    refute Readiness.ready?()
  end
end
