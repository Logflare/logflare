defmodule LogflareGrpc.Endpoint do
  use GRPC.Endpoint

  intercept(GRPC.Server.Interceptors.Logger, level: :debug)
  intercept LogflareGrpc.HttpProtobufInterceptor
  run(LogflareGrpc.Trace.Server)
end
