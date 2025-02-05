defmodule LogflareGrpc.Endpoint do
  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger, level: :info)
  run(LogflareGrpc.Trace.Server)
end
