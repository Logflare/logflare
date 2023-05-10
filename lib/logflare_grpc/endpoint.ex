defmodule LogflareGrpc.Endpoint do
  use GRPC.Endpoint

  run(LogflareGrpc.Trace.Server)
end
