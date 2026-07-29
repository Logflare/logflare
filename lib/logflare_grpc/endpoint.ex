defmodule LogflareGrpc.Endpoint do
  use GRPC.Endpoint

  run(LogflareGrpc.Trace.Server,
    interceptors: [LogflareGrpc.Interceptors.VerifyApiResourceAccess]
  )

  run(LogflareGrpc.Metrics.Server,
    interceptors: [LogflareGrpc.Interceptors.VerifyApiResourceAccess]
  )

  run(LogflareGrpc.Logs.Server,
    interceptors: [LogflareGrpc.Interceptors.VerifyApiResourceAccess]
  )

  run(LogflareGrpc.Health.Server)
end
