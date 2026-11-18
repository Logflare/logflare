defmodule Logflare.IngressDrain do
  @moduledoc false

  require Logger

  alias Logflare.IngressGate

  @spec run(integer(), keyword()) :: :drained | :timeout | :error
  def run(deadline, opts \\ []) do
    IngressGate.quiesce()

    http = suspend(:http, fn -> suspend_http(opts) end)
    grpc = suspend(:grpc, fn -> suspend_grpc(opts, deadline) end)
    wait = IngressGate.await_empty(deadline)

    result =
      case {wait, http, grpc} do
        {:timeout, _, _} -> :timeout
        {:drained, :ok, :ok} -> :drained
        _ -> :error
      end

    Logger.warning(
      "shutdown: ingress result=#{result} active_requests=#{IngressGate.active_count()}"
    )

    result
  end

  defp suspend_http(opts) do
    opts
    |> Keyword.get_lazy(:http_listeners, &http_listeners/0)
    |> Enum.map(fn pid -> suspend(pid, fn -> ThousandIsland.suspend(pid) end) end)
    |> Enum.find(:ok, &(&1 != :ok))
  end

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

  defp suspend_grpc(opts, deadline) do
    ref = Keyword.get(opts, :grpc_listener, inspect(LogflareGrpc.Endpoint))

    case :ranch.info() do
      %{^ref => _info} ->
        budget =
          suspend(:ranch_shutdown_budget, fn ->
            remaining = max(deadline - System.monotonic_time(:millisecond), 1)
            transport_opts = :ranch.get_transport_options(ref)
            :ranch.set_transport_options(ref, Map.put(transport_opts, :shutdown, remaining))
          end)

        listener = :ranch.suspend_listener(ref)
        if listener == :ok, do: budget, else: listener

      _ ->
        :ok
    end
  end
end
