defmodule LogflareGrpc.Interceptors.Shutdown do
  @moduledoc false
  @behaviour GRPC.Server.Interceptor

  alias Logflare.IngressGate

  @impl true
  def init(opts), do: opts

  @impl true
  def call(request, stream, next, _opts) do
    case IngressGate.enter() do
      {:ok, token} ->
        try do
          next.(request, stream)
        after
          IngressGate.leave(token)
        end

      :closed ->
        {:error, GRPC.RPCError.exception(status: :unavailable, message: "Server shutting down")}
    end
  end
end
