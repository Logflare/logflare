defmodule Logflare.SigtermHandlerTest do
  use ExUnit.Case, async: false

  alias Logflare.Application
  alias Logflare.Readiness
  alias Logflare.SigtermHandler

  setup do
    Readiness.mark_ready()
    on_exit(&Readiness.mark_ready/0)
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
end
