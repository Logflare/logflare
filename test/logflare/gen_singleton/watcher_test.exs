defmodule Logflare.GenSingleton.WatcherTest do
  use ExUnit.Case, async: true
  use GenServer

  import ExUnit.CaptureLog

  alias Logflare.GenSingleton.Watcher

  @password "watcher-dummy-password"

  @impl GenServer
  def init(options), do: {:stop, Keyword.fetch!(options, :failure)}

  test "failed starts do not log the child specification's credentials" do
    log = failed_start_log(:econnrefused)

    assert log =~ "failed to start child (reason: :econnrefused)"
    refute log =~ @password
    refute log =~ "epgsql"
  end

  test "PostgreSQL errors retain the error code name but not server text" do
    error = {:error, :fatal, "28P01", :invalid_password, @password, [detail: @password]}
    log = failed_start_log(error)

    assert log =~ "reason: :invalid_password"
    refute log =~ @password
  end

  test "unrecognized errors do not expose exception messages or stacktrace arguments" do
    error = {%RuntimeError{message: @password}, [{__MODULE__, :init, [@password], []}]}
    log = failed_start_log(error)

    assert log =~ "reason: :unknown"
    refute log =~ @password
  end

  defp failed_start_log(reason) do
    supervisor =
      start_supervised!(%{
        id: Supervisor,
        start: {Supervisor, :start_link, [[], [strategy: :one_for_one]]}
      })

    spec = %{
      id: __MODULE__,
      start:
        {GenServer, :start_link, [__MODULE__, [failure: reason, epgsql: %{password: @password}]]},
      restart: :temporary
    }

    state = %{
      sup_pid: supervisor,
      child_spec: spec,
      monitor_ref: nil,
      monitor_pid: nil
    }

    capture_log([level: :warning], fn ->
      assert {:noreply, ^state} = Watcher.handle_info(:maybe_start_child, state)
    end)
  end
end
