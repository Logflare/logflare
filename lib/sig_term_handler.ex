defmodule Logflare.SigtermHandler do
  @moduledoc false
  @behaviour :gen_event
  require Logger

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
    Logger.warning(
      "#{__MODULE__}: SIGTERM received: waiting for #{env_grace_period() / 1_000} seconds"
    )

    # Not sure something is causing the cluster to have issues when an instance gets shutdown
    :rpc.eval_everywhere(Node.list(), :erlang, :disconnect_node, [Node.self()])

    Process.send_after(self(), :proceed_with_sigterm, env_grace_period())

    {:ok, state}
  end

  def handle_event(:sigquit, state) do
    Logger.warning(
      "#{__MODULE__}: SIGQUIT received: waiting for #{env_grace_period() / 1_000} seconds"
    )

    Process.send_after(self(), :proceed_with_sigterm, env_grace_period())

    {:ok, state}
  end

  @impl true
  def handle_event(ev, state) do
    Logger.warning("#{__MODULE__}: has received a system signal: #{ev} and is ignoring it")

    {:ok, state}
  end

  @impl true
  def handle_call(msg, state) do
    Logger.warning("#{__MODULE__} has received an unexpected call: #{inspect(msg)}")
    {:ok, :ok, state}
  end
end
