defmodule Logflare.SigtermHandlerTest do
  use ExUnit.Case, async: false

  alias Logflare.Readiness
  alias Logflare.SigtermHandler

  test "application stop marks readiness false and it cannot become ready again" do
    Readiness.initialize()
    Readiness.mark_ready()

    on_exit(fn ->
      Readiness.initialize()
      Readiness.mark_ready()
    end)

    assert Logflare.Application.prep_stop(:state) == :state
    refute Readiness.ready?()
    Readiness.mark_ready()
    refute Readiness.ready?()
  end

  test "ignores repeated or mixed termination signals after requesting shutdown" do
    assert {:ok, :stopping} = SigtermHandler.handle_event(:sigterm, :stopping)
    assert {:ok, :stopping} = SigtermHandler.handle_event(:sigquit, :stopping)
  end

  test "ignores unrelated events without starting shutdown" do
    assert {:ok, :running} = SigtermHandler.init([])
    assert {:ok, :running} = SigtermHandler.handle_event(:other, :running)
  end
end
