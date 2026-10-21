defmodule Logflare.SigtermHandler do
  @moduledoc false
  @behaviour :gen_event
  require Logger

  alias Logflare.Readiness
  alias Logflare.ShutdownDrain

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

  defp drain_timeout_ms do
    case Application.get_env(:logflare, :shutdown_drain_timeout_ms) do
      ms when is_integer(ms) -> max(ms, env_grace_period())
      nil -> nil
    end
  end

  defp log_received(signal) do
    if timeout_ms = drain_timeout_ms() do
      Logger.warning(
        "#{__MODULE__}: #{signal} received: draining for up to #{timeout_ms / 1_000} seconds " <>
          "before stopping"
      )
    else
      Logger.warning(
        "#{__MODULE__}: #{signal} received: waiting for #{env_grace_period() / 1_000} seconds"
      )
    end
  end

  # With a drain timeout configured, the node drains what it holds before
  # stopping; the drain itself waits out the grace period first.
  defp schedule_stop do
    grace_period = env_grace_period()

    if timeout_ms = drain_timeout_ms() do
      handler = self()

      Task.start(fn ->
        try do
          result = ShutdownDrain.run(timeout_ms: timeout_ms, min_wait_ms: grace_period)

          Logger.warning(
            "#{__MODULE__}: shutdown drain ended with #{inspect(result)}, stopping the app"
          )
        after
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
