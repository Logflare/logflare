defmodule LogflareGrpc.HttpProtobufInterceptor do
  @moduledoc false

  @behaviour GRPC.Server.Interceptor

  def init(opts) do
    opts
  end

  def call(rpc_req, stream, next, opts) do
    dbg({rpc_req, stream, next, opts})
    {:ok, stream}
  end
end
