defmodule Logflare.IngressDrain do
  @moduledoc false

  require Logger

  alias Logflare.IngressGate
  alias Logflare.Shutdown.Budget
  alias Logflare.Utils.Tasks

  @grpc_goaway_ms 5_000
  @grpc_wire_timeout_ms 6_000

  @spec grpc_protocol_options() :: keyword()
  def grpc_protocol_options do
    [
      goaway_initial_timeout: 0,
      goaway_complete_timeout: @grpc_goaway_ms,
      shutdown_timeout: @grpc_goaway_ms
    ]
  end

  @spec run(integer(), keyword()) :: :drained | :timeout | :error
  def run(deadline, opts \\ []) do
    IngressGate.quiesce()

    listener_deadline = Keyword.get(opts, :listener_deadline, deadline)
    listeners = suspend_listeners(listener_deadline, opts)
    wait = IngressGate.await_empty(deadline)

    result =
      cond do
        :timeout in [wait | listeners] -> :timeout
        :error in listeners -> :error
        true -> :drained
      end

    Logger.warning(
      "shutdown: ingress result=#{result} active_requests=#{IngressGate.active_count()}"
    )

    result
  end

  defp suspend_listeners(deadline, opts) do
    actions = [
      http: fn -> suspend_http(opts) end,
      grpc: fn -> suspend_grpc(Keyword.get_lazy(opts, :grpc_listener, &grpc_listener/0)) end
    ]

    tasks =
      Enum.map(actions, fn {name, fun} -> Tasks.async_nolink(fn -> suspend(name, fun) end) end)

    results = Task.yield_many(tasks, timeout: Budget.remaining(deadline), on_timeout: :kill_task)

    Enum.zip_with(Keyword.keys(actions), results, &suspension_result/2)
  end

  defp suspension_result(_name, {_task, {:ok, result}}), do: result

  defp suspension_result(name, {_task, {:exit, reason}}), do: log_suspend_error(name, reason)

  defp suspension_result(name, {_task, nil}) do
    log_suspend_error(name, :timeout)
    :timeout
  end

  defp suspend_http(opts) do
    opts
    |> Keyword.get_lazy(:http_listeners, &http_listeners/0)
    |> Enum.map(&suspend_http_listener/1)
    |> Enum.find(:ok, &(&1 != :ok))
  end

  defp suspend_http_listener(pid), do: suspend(pid, fn -> ThousandIsland.suspend(pid) end)

  defp suspend(listener, fun) do
    case fun.() do
      :ok -> :ok
      error -> log_suspend_error(listener, error)
    end
  rescue
    error in ArgumentError -> log_suspend_error(listener, Exception.message(error))
  catch
    :exit, reason -> log_suspend_error(listener, reason)
  end

  defp log_suspend_error(listener, reason) do
    Logger.warning(
      "shutdown: failed to suspend listener #{inspect(listener)}: #{inspect(reason)}"
    )

    :error
  end

  defp http_listeners do
    for scheme <- [:http, :https],
        {:ok, pid} <- [Bandit.PhoenixAdapter.bandit_pid(LogflareWeb.Endpoint, scheme)],
        is_pid(pid),
        do: pid
  end

  defp grpc_listener do
    ref = inspect(LogflareGrpc.Endpoint)
    if Map.has_key?(:ranch.info(), ref), do: ref
  end

  defp suspend_grpc(nil), do: :ok

  defp suspend_grpc(ref) do
    budget = suspend({:grpc_wire_budget, ref}, fn -> set_grpc_wire_budget(ref) end)
    listener = suspend(ref, fn -> :ranch.suspend_listener(ref) end)
    if budget == :ok and listener == :ok, do: :ok, else: :error
  end

  defp set_grpc_wire_budget(ref) do
    opts = :ranch.get_transport_options(ref)
    :ranch.set_transport_options(ref, Map.put(opts, :shutdown, @grpc_wire_timeout_ms))
  end
end
