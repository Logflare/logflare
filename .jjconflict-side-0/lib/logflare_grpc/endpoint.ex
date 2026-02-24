defmodule LogflareGrpc.Endpoint do
  use GRPC.Endpoint

  intercept(LogflareGrpc.Interceptors.VerifyApiResourceAccess)
  run(LogflareGrpc.Trace.Server)
  run(LogflareGrpc.Metrics.Server)
  run(LogflareGrpc.Logs.Server)
end
