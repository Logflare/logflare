defmodule LogflareGrpc.ExceptionLogFilter do
  @moduledoc """
  Filters expected `GRPC.RPCError` rejections (bad api keys, missing sources,
  insufficient scopes, a deleted source, a source that is still starting) out of the crash-level
  exception logs emitted by `GRPC.Server.Adapters.Cowboy.Handler`, since they are
  normal control flow rather than bugs. Unexpected exceptions are still logged.

  `GRPC.RPCError.exception/1` stores the status as an integer, so the filter
  matches the integer codes from `GRPC.Status`.
  """

  alias GRPC.Server.Adapters.ReportException

  @expected_statuses [
    GRPC.Status.not_found(),
    GRPC.Status.permission_denied(),
    GRPC.Status.unauthenticated(),
    GRPC.Status.unavailable()
  ]

  @spec emit_log?(%ReportException{}) :: boolean()
  def emit_log?(%ReportException{reason: %GRPC.RPCError{status: status}})
      when status in @expected_statuses,
      do: false

  def emit_log?(%ReportException{}), do: true
end
