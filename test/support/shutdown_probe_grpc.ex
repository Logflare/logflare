defmodule Logflare.Test.ShutdownProbeGrpc do
  use GRPC.Server, service: Opentelemetry.Proto.Collector.Logs.V1.LogsService.Service

  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  @spec export(term(), GRPC.Server.Stream.t()) :: ExportLogsServiceResponse.t()
  def export(_request, _stream) do
    test_pid = :persistent_term.get({__MODULE__, :test_pid})
    send(test_pid, {:held_rpc, self()})

    receive do
      :release -> %ExportLogsServiceResponse{}
    end
  end
end
