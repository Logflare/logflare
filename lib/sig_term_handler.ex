defmodule Logflare.SigtermHandler do
  @moduledoc false
  @behaviour :gen_event

  require Logger

  alias Logflare.IngressGate
  alias Logflare.Readiness
  alias Logflare.Shutdown

  @impl true
  def init(_), do: {:ok, %{}}

  @impl true
  def handle_event(signal, %{shutdown_started: true} = state)
      when signal in [:sigterm, :sigquit] do
    Logger.warning("#{__MODULE__}: ignoring repeated #{signal} during shutdown")
    {:ok, state}
  end

  def handle_event(signal, state) when signal in [:sigterm, :sigquit] do
    Readiness.begin_draining()
    withdrawal_ms = Application.fetch_env!(:logflare, :sigterm_shutdown_grace_period_ms)
    drain_ms = Application.get_env(:logflare, :shutdown_drain_timeout_ms)
    budget_ms = Shutdown.timeout_ms()
    deadline = System.monotonic_time(:millisecond) + budget_ms
    handler = self()

    Logger.warning(
      "#{__MODULE__}: #{signal} received: shutdown budget=#{budget_ms}ms withdrawal=#{withdrawal_ms}ms"
    )

    timer = Process.send_after(handler, :shutdown_deadline, budget_ms)

    {:ok, worker} =
      Task.start(fn ->
        try do
          result =
            Shutdown.run(
              deadline_ms: deadline,
              withdrawal_ms: withdrawal_ms,
              request_timeout_ms:
                Application.get_env(:logflare, :shutdown_request_timeout_ms, 30_000),
              drain_timeout_ms: drain_ms
            )

          Logger.warning(
            "#{__MODULE__}: shutdown ended with #{inspect(result)}, stopping the app"
          )
        after
          send(handler, :proceed_with_sigterm)
        end
      end)

    {:ok, Map.merge(state, %{shutdown_started: true, worker: worker, timer: timer})}
  end

  def handle_event(event, state) do
    Logger.warning("#{__MODULE__}: ignoring system signal #{inspect(event)}")
    {:ok, state}
  end

  @impl true
  def handle_info(event, %{stopping: true} = state)
      when event in [:shutdown_deadline, :proceed_with_sigterm],
      do: {:ok, state}

  def handle_info(:shutdown_deadline, state) do
    Logger.warning("#{__MODULE__}: shutdown deadline exceeded; forcing application stop")
    Process.exit(state.worker, :kill)
    stop(state)
  end

  def handle_info(:proceed_with_sigterm, state), do: stop(state)
  def handle_info(_event, state), do: {:ok, state}

  @impl true
  def handle_call(message, state) do
    Logger.warning("#{__MODULE__}: unexpected call #{inspect(message)}")
    {:ok, :ok, state}
  end

  defp stop(state) do
    Process.cancel_timer(state.timer)
    IngressGate.close()
    System.stop()
    {:ok, Map.put(state, :stopping, true)}
  end
end
