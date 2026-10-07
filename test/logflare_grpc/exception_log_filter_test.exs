defmodule LogflareGrpc.ExceptionLogFilterTest do
  use ExUnit.Case, async: true

  alias GRPC.Server.Adapters.ReportException
  alias LogflareGrpc.ExceptionLogFilter

  describe "emit_log?/1" do
    for status <- [:permission_denied, :unauthenticated, :unavailable] do
      test "returns false for a #{status} GRPC.RPCError rejection" do
        error = GRPC.RPCError.exception(status: unquote(status))
        exception = ReportException.new([req: :ok], error)

        refute ExceptionLogFilter.emit_log?(exception)
      end
    end

    test "returns true for a GRPC.RPCError with an unrecognized status" do
      exception = ReportException.new([req: :ok], GRPC.RPCError.exception(status: :internal))

      assert ExceptionLogFilter.emit_log?(exception)
    end

    test "returns true for unexpected exceptions raised by handlers" do
      exception = ReportException.new([req: :ok], %RuntimeError{message: "boom"})

      assert ExceptionLogFilter.emit_log?(exception)
    end
  end
end
