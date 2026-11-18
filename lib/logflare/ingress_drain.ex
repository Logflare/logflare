defmodule Logflare.IngressDrain do
  @moduledoc false

  require Logger

  alias Logflare.IngressGate

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

    http = suspend(:http, fn -> suspend_http(opts) end)

    grpc =
      suspend(:grpc, fn ->
        suspend_grpc(Keyword.get_lazy(opts, :grpc_listener, &grpc_listener/0))
      end)

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
