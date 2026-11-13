defmodule Logflare.IngressDrain do
  @moduledoc false

  require Logger

  alias Logflare.IngressGate

  @spec run(integer(), keyword()) :: :drained | :timeout
  def run(deadline, opts \\ []) do
    IngressGate.close()

    for pid <- Keyword.get_lazy(opts, :http_listeners, &http_listeners/0) do
      :ok = ThousandIsland.suspend(pid)
    end

    suspend_grpc(Keyword.get(opts, :grpc_listener, inspect(LogflareGrpc.Endpoint)), deadline)
    result = IngressGate.await_empty(deadline)

    Logger.warning(
      "shutdown: ingress result=#{result} active_requests=#{IngressGate.active_count()}"
    )

    result
  end

  defp http_listeners do
    for scheme <- [:http, :https],
        {:ok, pid} <- [Bandit.PhoenixAdapter.bandit_pid(LogflareWeb.Endpoint, scheme)],
        is_pid(pid),
        do: pid
  end

  defp suspend_grpc(ref, deadline) do
    case :ranch.info() do
      %{^ref => _info} ->
        remaining = max(deadline - System.monotonic_time(:millisecond), 1)
        transport_opts = :ranch.get_transport_options(ref)
        :ok = :ranch.set_transport_options(ref, Map.put(transport_opts, :shutdown, remaining))
        :ranch.suspend_listener(ref)

      _ ->
        :ok
    end
  end
end
