defmodule LogflareGrpc.Interceptors.Shutdown do
  @moduledoc false
  @behaviour GRPC.Server.Interceptor

  alias Logflare.IngressGate

  @impl true
  def init(opts), do: opts

  @impl true
  def call(request, stream, next, _opts) do
    IngressGate.with_request(fn -> next.(request, stream) end, fn ->
      {:error, GRPC.RPCError.exception(status: :unavailable, message: "Server shutting down")}
    end)
  end
end
