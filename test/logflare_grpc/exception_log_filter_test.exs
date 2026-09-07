defmodule LogflareGrpc.ExceptionLogFilterTest do
  use ExUnit.Case, async: true

  alias GRPC.Server.Adapters.ReportException
  alias LogflareGrpc.ExceptionLogFilter

  describe "emit_log?/1" do
    test "returns false for a permission_denied GRPC.RPCError rejection" do
      exception = ReportException.new([req: :ok], %GRPC.RPCError{status: :permission_denied})

      refute ExceptionLogFilter.emit_log?(exception)
    end

    test "returns false for an unauthenticated GRPC.RPCError rejection" do
      exception = ReportException.new([req: :ok], %GRPC.RPCError{status: :unauthenticated})

      refute ExceptionLogFilter.emit_log?(exception)
    end

    test "returns true for a GRPC.RPCError with an unrecognized status" do
      exception = ReportException.new([req: :ok], %GRPC.RPCError{status: :internal})

      assert ExceptionLogFilter.emit_log?(exception)
    end

    test "returns true for unexpected exceptions raised by handlers" do
      exception = ReportException.new([req: :ok], %RuntimeError{message: "boom"})

      assert ExceptionLogFilter.emit_log?(exception)
    end
  end
end
