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
    IngressGate.close()
  end

  def terminate(_reason, _timeout), do: :ok

  @spec timeout_ms() :: non_neg_integer()
  def timeout_ms do
    withdrawal = Application.fetch_env!(:logflare, :sigterm_shutdown_grace_period_ms)
    drain = Application.get_env(:logflare, :shutdown_drain_timeout_ms, 0)
    Application.get_env(:logflare, :shutdown_timeout_ms, max(withdrawal, drain || 0) + 20_000)
  end

  @spec run(keyword()) :: :drained | :timeout
  def run(opts) do
    deadline = Keyword.fetch!(opts, :deadline_ms)
    withdrawal_ms = Keyword.fetch!(opts, :withdrawal_ms)
    request_timeout_ms = Keyword.fetch!(opts, :request_timeout_ms)
    drain_timeout_ms = Keyword.fetch!(opts, :drain_timeout_ms)

    ShutdownDrain.stop_receiving()
    Logger.warning("shutdown: withdrawing from traffic for #{withdrawal_ms}ms")
    Process.sleep(min(withdrawal_ms, remaining(deadline)))

    ingress_deadline = min(deadline, System.monotonic_time(:millisecond) + request_timeout_ms)
    ingress_result = IngressDrain.run(ingress_deadline)

    drain_result =
      if drain_timeout_ms do
        ShutdownDrain.run(
          timeout_ms: min(drain_timeout_ms, remaining(deadline)),
          deadline_ms: deadline
        )
      else
        :drained
      end

    if ingress_result == :drained and drain_result == :drained and
         System.monotonic_time(:millisecond) <= deadline,
       do: :drained,
       else: :timeout
  end

  defp remaining(deadline), do: max(deadline - System.monotonic_time(:millisecond), 0)
end
