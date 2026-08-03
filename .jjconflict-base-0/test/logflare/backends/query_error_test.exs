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

    test "logs BigQuery reservation backend errors with raw backend detail" do
      for {message, expected_detail} <- [
            {
              "User specified reservation projects/p/locations/l/reservations/missing is not found",
              "projects/p/locations/l/reservations/missing"
            },
            {
              "Access Denied: Reservation projects/p/locations/l/reservations/r: Permission bigquery.reservations.use denied on reservation projects/p/locations/l/reservations/r (or it may not exist)",
              "bigquery.reservations.use denied"
            },
            {
              "Cannot run query: project does not have the reservation in the data region or no slots are configured",
              "no slots are configured"
            }
          ] do
        error =
          query_error(
            kind: :backend_error,
            raw_error: %{
              "message" => message,
              "status" => "FAILED_PRECONDITION"
            }
          )

        log =
          capture_log(
            [
              level: :error,
              metadata: [:user_id, :bigquery_project_id, :backend, :error_kind, :error_string]
            ],
            fn ->
              assert ^error =
                       QueryError.log(error,
                         user_id: 123,
                         bigquery_project_id: "test-project"
                       )
            end
          )

        assert log =~ "Backend query error"
        assert log =~ "user_id=123"
        assert log =~ "bigquery_project_id=test-project"
        assert log =~ "backend=Logflare.Backends.Adaptor.BigQueryAdaptor"
        assert log =~ "error_kind=backend_error"
        assert log =~ expected_detail
        refute log =~ "Possible BigQuery reservation error"
      end
    end

    test "does not log invalid query errors when raw detail just mentions the word reservation" do
      error =
        query_error(
          raw_error: %{
            "message" =>
              "User specified reservation projects/p/locations/l/reservations/missing is not found"
          }
        )

      log =
        capture_log([level: :error, metadata: [:error_kind, :error_string]], fn ->
          assert ^error = QueryError.log(error)
        end)

      assert log == ""
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
