defmodule Logflare.Shutdown do
  @moduledoc """
  Drains ingress and ingestion before the supervisor stops their dependencies.
  Start this child last so it is stopped first; the child shutdown timeout
  bounds the drain even if a dependency stops responding.
  """

  use GenServer

  require Logger

  alias Logflare.IngressDrain
  alias Logflare.IngressGate
  alias Logflare.Shutdown.Budget
  alias Logflare.ShutdownDrain

  @spec child_spec(keyword()) :: Supervisor.child_spec()
  def child_spec([]) do
    timeout = timeout_ms()
    %{id: __MODULE__, start: {__MODULE__, :start_link, [timeout]}, shutdown: timeout}
  end

  @spec start_link(non_neg_integer()) :: GenServer.on_start()
  def start_link(timeout), do: GenServer.start_link(__MODULE__, timeout)

  @impl GenServer
  def init(timeout) do
    Process.flag(:trap_exit, true)
    {:ok, timeout}
  end

  @impl GenServer
  def terminate(:shutdown, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    Logger.warning("shutdown: starting supervised drain with budget=#{timeout}ms")

    result =
      run(
        deadline_ms: deadline,
        withdrawal_ms: Application.fetch_env!(:logflare, :sigterm_shutdown_grace_period_ms),
        request_timeout_ms: Application.get_env(:logflare, :shutdown_request_timeout_ms, 30_000),
        drain_timeout_ms: Application.get_env(:logflare, :shutdown_drain_timeout_ms)
      )

    Logger.warning("shutdown: supervised drain finished result=#{result}")
  after
    IngressGate.seal()
  end

  def terminate(_reason, _timeout), do: :ok

  @spec timeout_ms() :: non_neg_integer()
  def timeout_ms do
    withdrawal = Application.fetch_env!(:logflare, :sigterm_shutdown_grace_period_ms)
    drain = Application.get_env(:logflare, :shutdown_drain_timeout_ms, 0)
    Application.get_env(:logflare, :shutdown_timeout_ms, max(withdrawal, drain || 0) + 20_000)
  end

  @spec run(keyword()) :: :drained | :timeout | :error
  def run(opts) do
    deadline = Keyword.fetch!(opts, :deadline_ms)
    buffer = ShutdownDrain.spool_buffer()

    budget =
      Budget.plan(System.monotonic_time(:millisecond), Keyword.put(opts, :spool_buffer, buffer))

    intake = ShutdownDrain.stop_receiving(Budget.deadline(budget.intake))
    withdrawal_ms = Budget.remaining(budget.withdrawal)
    Logger.warning("shutdown: withdrawing from traffic for #{withdrawal_ms}ms")
    Process.sleep(withdrawal_ms)

    ingress_result =
      IngressDrain.run(Budget.deadline(budget.requests),
        listener_deadline: Budget.deadline(budget.listeners)
      )

    IngressGate.seal()

    case IngressGate.await_writes(Budget.deadline(budget.writes)) do
      :drained ->
        result = ShutdownDrain.run(budget.flush, intake, buffer)
        finish(ingress_result, result, deadline)

      :timeout ->
        Logger.warning("shutdown: ingestion handoffs still active; skipping final drain")
        :timeout
    end
  end

  defp finish(ingress_result, drain_result, deadline) do
    cond do
      :timeout in [ingress_result, drain_result] or Budget.remaining(deadline) == 0 -> :timeout
      ingress_result == :error -> :error
      true -> :drained
    end
  end
end
