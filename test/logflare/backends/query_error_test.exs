defmodule Logflare.Backends.QueryErrorTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.QueryError

  describe "struct" do
    test "stores backend, kind, and raw error detail" do
      error = %QueryError{
        kind: :invalid_query,
        raw_error: %{"message" => "raw backend message"},
        backend: Logflare.Backends.Adaptor.BigQueryAdaptor,
        description: "user-facing description"
      }

      assert error.kind == :invalid_query
      assert error.raw_error == %{"message" => "raw backend message"}
      assert error.backend == BigQueryAdaptor
    end
  end

  describe "log/2" do
    test "does not log invalid query errors" do
      error = query_error(raw_error: %{"message" => "raw user query detail"})

      log =
        capture_log(
          [level: :error, metadata: [:user_id, :backend_id, :error_kind, :error_string]],
          fn ->
            assert ^error =
                     QueryError.log(error,
                       user_id: 123,
                       backend_id: 456,
                       source_token: nil
                     )
          end
        )

      assert log == ""
    end

    test "logs backend errors with user metadata and raw backend detail" do
      error = query_error(kind: :backend_error, raw_error: %{"message" => "raw backend detail"})

      log =
        capture_log(
          [level: :error, metadata: [:user_id, :backend_id, :error_kind, :error_string]],
          fn ->
            assert ^error =
                     QueryError.log(error,
                       user_id: 123,
                       backend_id: 456,
                       source_token: nil
                     )
          end
        )

      assert log =~ "Backend query error"
      assert log =~ "user_id=123"
      assert log =~ "backend_id=456"
      assert log =~ "error_kind=backend_error"
      assert log =~ "raw backend detail"
      refute log =~ "source_token="
    end

    test "logs query errors without requiring user metadata" do
      error = query_error(raw_error: :timeout, kind: :connection_error)

      log =
        capture_log([level: :error, metadata: [:user_id, :error_kind, :error_string]], fn ->
          assert ^error = QueryError.log(error)
        end)

      assert log =~ "Backend query error"
      assert log =~ "error_kind=connection_error"
      assert log =~ "timeout"
      refute log =~ "user_id="
    end
  end

  defp query_error(attrs) do
    attrs =
      Keyword.merge(
        [
          kind: :invalid_query,
          raw_error: %{"message" => "backend failed"},
          backend: BigQueryAdaptor,
          description: nil
        ],
        attrs
      )

    struct!(QueryError, attrs)
  end
end
