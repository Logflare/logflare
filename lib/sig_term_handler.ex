defmodule Logflare.SigtermHandler do
  @moduledoc false
  @behaviour :gen_event
  require Logger

  alias Logflare.Backends
  alias Logflare.Backends.Spool.ConsumerDrain
  alias Logflare.Readiness

  defp env_grace_period,
    do:
      Application.get_env(:logflare, :sigterm_shutdown_grace_period_ms) ||
        throw("Not configured")

  @impl true
  def init(_) do
    {:ok, %{}}
  end

  @impl true
  def handle_info(:proceed_with_sigterm, state) do
    Logger.warning("#{__MODULE__}: shutdown grace period reached, stopping the app...")

    System.stop()

    {:ok, state}
  end

  @impl true
  def handle_event(:sigterm, state) do
    Readiness.begin_draining()

    log_received("SIGTERM")

    # Not sure something is causing the cluster to have issues when an instance gets shutdown
    :rpc.eval_everywhere(Node.list(), :erlang, :disconnect_node, [Node.self()])

    schedule_stop()

    {:ok, state}
  end

  def handle_event(:sigquit, state) do
    Readiness.begin_draining()

    log_received("SIGQUIT")

    schedule_stop()

    {:ok, state}
  end

  @impl true
  def handle_event(ev, state) do
    Logger.warning("#{__MODULE__}: has received a system signal: #{ev} and is ignoring it")

    {:ok, state}
  end

  defp log_received(signal) do
    if Backends.spool_consumer_mode?() do
      Logger.warning(
        "#{__MODULE__}: #{signal} received: draining spool consumer for up to " <>
          "#{ConsumerDrain.timeout_ms() / 1_000} seconds before stopping"
      )
    else
      Logger.warning(
        "#{__MODULE__}: #{signal} received: waiting for #{env_grace_period() / 1_000} seconds"
      )
    end
  end

  # A spool consumer drains its in-flight files before stopping, and still
  # waits out the grace period if it finishes sooner.
  defp schedule_stop do
    grace_period = env_grace_period()

    if Backends.spool_consumer_mode?() do
      handler = self()

      Task.start(fn ->
        started_at = System.monotonic_time(:millisecond)

        try do
          result = ConsumerDrain.run()

          Logger.warning(
            "#{__MODULE__}: spool consumer drain ended with #{inspect(result)}, stopping the app"
          )
        after
          elapsed = System.monotonic_time(:millisecond) - started_at
          Process.sleep(max(grace_period - elapsed, 0))
          send(handler, :proceed_with_sigterm)
        end
      end)
    else
      Process.send_after(self(), :proceed_with_sigterm, grace_period)
    end
  end

  @impl true
  def handle_call(msg, state) do
    Logger.warning("#{__MODULE__} has received an unexpected call: #{inspect(msg)}")
    {:ok, :ok, state}
  end
end
