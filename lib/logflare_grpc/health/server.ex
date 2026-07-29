defmodule LogflareGrpc.Health.Server do
  @moduledoc """
  Implements the standard `grpc.health.v1.Health` service so that
  infrastructure (e.g. an AWS ALB gRPC target group) can health-check
  the gRPC listener without needing an API key.
  """

  use GRPC.Server, service: Grpc.Health.V1.Health.Service

  alias Grpc.Health.V1.HealthCheckRequest
  alias Grpc.Health.V1.HealthCheckResponse

  @spec check(HealthCheckRequest.t(), GRPC.Server.Stream.t()) :: HealthCheckResponse.t()
  def check(_request, _stream) do
    %HealthCheckResponse{status: :SERVING}
  end

  @spec watch(HealthCheckRequest.t(), GRPC.Server.Stream.t()) :: no_return()
  def watch(_request, _stream) do
    raise GRPC.RPCError, status: :unimplemented
  end
end
