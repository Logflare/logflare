defmodule Logflare.SigtermHandler do
  @moduledoc false
  @behaviour :gen_event
  require Logger

  @grace_period Application.get_env(:logflare, :sigterm_shutdown_grace_period_ms) ||
                  throw("Not configured")

  @impl true
  def init(_) do
    Logger.info("#{__MODULE__} is being initialized...")
    {:ok, %{}}
  end

  @impl true
  def handle_info(:proceed_with_sigterm, state) do
    Logger.warn("#{__MODULE__}: shutdown grace period reached, stopping the app...")
    :init.stop()
    {:ok, state}
  end

  @impl true
  def handle_event(:sigterm, state) do
    Logger.warn("#{__MODULE__}: SIGTERM received: waiting for #{@grace_period / 1_000} seconds")
    Process.send_after(self(), :proceed_with_sigterm, @grace_period)

    {:ok, state}
  end

  @impl true
  def handle_event(ev, _state) do
    Logger.warn("#{__MODULE__}: has received a system signal: #{ev} and redirected it to :erl_signal_server")
    :gen_event.notify(:erl_signal_server, ev)
  end

  @impl true
  def handle_call(msg, state) do
    Logger.warn("#{__MODULE__} has received an unexpected call: #{inspect(msg)}")
    {:ok, :ok, state}
  end
end
