defmodule Logflare.SigtermHandler do
  @moduledoc false
  @behaviour :gen_event

  require Logger

  alias Logflare.Readiness

  @impl true
  def init(_), do: {:ok, :running}

  @impl true
  def handle_event(signal, :stopping) when signal in [:sigterm, :sigquit] do
    Logger.warning("#{__MODULE__}: ignoring repeated #{signal} during shutdown")
    {:ok, :stopping}
  end

  def handle_event(signal, :running) when signal in [:sigterm, :sigquit] do
    Readiness.begin_draining()
    Logger.warning("#{__MODULE__}: #{signal} received, requesting application stop")
    System.stop()
    {:ok, :stopping}
  end

  def handle_event(event, state) do
    Logger.warning("#{__MODULE__}: ignoring system signal #{inspect(event)}")
    {:ok, state}
  end

  @impl true
  def handle_call(message, state) do
    Logger.warning("#{__MODULE__}: unexpected call #{inspect(message)}")
    {:ok, :ok, state}
  end
end
