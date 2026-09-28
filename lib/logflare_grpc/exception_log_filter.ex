defmodule LogflareGrpc.ExceptionLogFilter do
  @moduledoc """
  Filters expected `GRPC.RPCError` rejections (bad api keys, missing sources,
  insufficient scopes, etc.) out of the crash-level exception logs emitted by
  `GRPC.Server.Adapters.Cowboy.Handler`, since they are normal control flow
  rather than bugs. Unexpected exceptions are still logged.
  """

  alias GRPC.Server.Adapters.ReportException

  @spec emit_log?(%ReportException{}) :: boolean()
  def emit_log?(%ReportException{reason: %GRPC.RPCError{status: :permission_denied}}), do: false
  def emit_log?(%ReportException{reason: %GRPC.RPCError{status: :unauthenticated}}), do: false
  def emit_log?(%ReportException{}), do: true
end
