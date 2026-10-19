defmodule LogflareWeb.QueryErrorHelpersTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.QueryError
  alias LogflareWeb.QueryErrorHelpers

  doctest LogflareWeb.QueryErrorHelpers

  describe "sandbox_query_error_message/1" do
    for message <- [
          "sql parser error: Expected: an expression, found: EOF",
          "Only SELECT queries allowed",
          "Only SELECT queries allowed in sandboxed queries",
          "Only singular query allowed",
          "Restricted function url",
          "Restricted setting max_threads",
          "restricted wildcard (*) in a result column"
        ] do
      @message message

      test "passes through #{inspect(message)}" do
        assert QueryErrorHelpers.sandbox_query_error_message(@message) == @message
      end
    end

    for message <- [
          "Multiple CTEs available (a, b). You must specify which one to query using `f:name`",
          "No CTEs found in query",
          "Table 'x' not found in available CTEs: a, b",
          "can't find source otel_logs_abc123def"
        ] do
      @message message

      test "hides #{inspect(message)}" do
        assert QueryErrorHelpers.sandbox_query_error_message(@message) ==
                 QueryErrorHelpers.generic_query_error_message()
      end
    end
  end

  describe "query_error_message/1" do
    test "returns a generic message for unclassified BigQuery errors" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %RuntimeError{message: "raw backend syntax error near SELECT secret_field"}
        )

      message = QueryErrorHelpers.query_error_message(error)

      assert message == QueryErrorHelpers.generic_query_error_message()
      refute message =~ "secret_field"
      refute message =~ "raw backend"
    end

    test "returns a generic message for unclassified ClickHouse errors" do
      error =
        query_error(
          backend: ClickHouseAdaptor,
          raw_error: %{message: "backend internal detail"}
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               QueryErrorHelpers.generic_query_error_message()
    end

    test "returns the adaptor-provided description for invalid queries" do
      error =
        query_error(
          backend: ClickHouseAdaptor,
          raw_error: %Ch.Error{code: 215, message: "raw ClickHouse message"},
          description: "Column 'a' is not under aggregate function. (NOT_AN_AGGREGATE)"
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               "Column 'a' is not under aggregate function. (NOT_AN_AGGREGATE)"
    end

    test "ignores a description on errors that are not invalid queries" do
      error =
        query_error(
          kind: :backend_error,
          backend: ClickHouseAdaptor,
          raw_error: %Ch.Error{code: 60, message: "raw ClickHouse message"},
          description: "internal detail"
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               QueryErrorHelpers.generic_query_error_message()
    end

    test "returns a generic message for unclassified Postgres errors" do
      error =
        query_error(
          backend: PostgresAdaptor,
          raw_error: %{message: "backend internal detail"}
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               QueryErrorHelpers.generic_query_error_message()
    end

    test "returns timeout message for connection timeouts" do
      error =
        query_error(
          kind: :connection_error,
          backend: BigQueryAdaptor,
          raw_error: :timeout
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               "Query timed out. Retry your query or reduce the time range."
    end

    test "returns timeout message for timeout query errors" do
      error =
        query_error(
          kind: :timeout,
          backend: BigQueryAdaptor,
          raw_error: %{
            "code" => 499,
            "errors" => [
              %{
                "domain" => "global",
                "message" => "Job execution was cancelled: Job timed out",
                "reason" => "stopped"
              }
            ],
            "message" => "Job execution was cancelled: Job timed out",
            "status" => "CANCELLED"
          }
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               "Query timed out. Retry your query or reduce the time range."
    end

    test "returns missing field message for classified query errors" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %{"message" => "Unrecognized name: notthere at [1:8]"}
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               ~s(Field "notthere" does not exist.)
    end

    test "returns BigQuery query without FROM clause invalid query message" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %{
            "code" => 400,
            "errors" => [
              %{
                "domain" => "global",
                "location" => "q",
                "locationType" => "parameter",
                "message" => "Query without FROM clause cannot have a WHERE clause at [1:47]",
                "reason" => "invalidQuery"
              }
            ],
            "message" => "Query without FROM clause cannot have a WHERE clause at [1:47]",
            "status" => "INVALID_ARGUMENT"
          }
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               "Query without FROM clause cannot have a WHERE clause at [1:47]"
    end

    test "returns bytes billed limit message for classified query errors" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %{
            "message" =>
              "Query exceeded limit for bytes billed: 2000000000. 20004857600 or higher required.",
            "reason" => "billingTierLimitExceeded"
          }
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               "total bytes processed for this query is expected to be greater than 2 GB"
    end

    test "falls back to generic message when bytes billed parsing fails" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %{
            "message" =>
              "Query exceeded limit for bytes billed but no numeric limit was returned",
            "reason" => "billingTierLimitExceeded"
          }
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               QueryErrorHelpers.generic_query_error_message()
    end
  end

  describe "timeout_query_error?/1" do
    test "is true for timeout query errors" do
      assert QueryErrorHelpers.timeout_query_error?(
               query_error(
                 kind: :timeout,
                 backend: BigQueryAdaptor,
                 raw_error: %{"message" => "Job execution was cancelled: Job timed out"}
               )
             )
    end

    test "is true for connection timeouts" do
      assert QueryErrorHelpers.timeout_query_error?(
               query_error(kind: :connection_error, backend: BigQueryAdaptor, raw_error: :timeout)
             )
    end

    test "is false for other query errors" do
      refute QueryErrorHelpers.timeout_query_error?(
               query_error(
                 kind: :connection_error,
                 backend: BigQueryAdaptor,
                 raw_error: :closed
               )
             )

      refute QueryErrorHelpers.timeout_query_error?(
               query_error(
                 kind: :backend_error,
                 backend: BigQueryAdaptor,
                 raw_error: %{"message" => "backend unavailable"}
               )
             )
    end
  end

  defp query_error(attrs) do
    attrs =
      Keyword.merge(
        [
          kind: :invalid_query,
          description: nil
        ],
        attrs
      )

    struct!(QueryError, attrs)
  end
end
