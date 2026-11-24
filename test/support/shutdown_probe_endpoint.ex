defmodule Logflare.Test.ShutdownProbeEndpoint do
  use GRPC.Endpoint

  intercept(LogflareGrpc.Interceptors.Shutdown)
  run(Logflare.Test.ShutdownProbeGrpc)
end
