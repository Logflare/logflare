defmodule LogflareWeb.QueryErrorHelpersTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.QueryError
  alias LogflareWeb.QueryErrorHelpers

  doctest LogflareWeb.QueryErrorHelpers

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

    test "returns a generic message for unclassified Postgres errors" do
      error =
        query_error(
          backend: PostgresAdaptor,
          raw_error: %{message: "backend internal detail"}
        )

      assert QueryErrorHelpers.query_error_message(error) ==
               QueryErrorHelpers.generic_query_error_message()
    end

    test "returns missing field message for classified query errors" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %{"message" => "Unrecognized name: notthere at [1:8]"}
        )

      assert QueryErrorHelpers.query_error_message(error) == ~s(Field "notthere" does not exist.)
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

  describe "query_halted_message/1" do
    test "prefixes classified messages" do
      error =
        query_error(
          backend: PostgresAdaptor,
          raw_error: %{message: ~s|column "notthere" does not exist|}
        )

      assert QueryErrorHelpers.query_halted_message(error) ==
               ~s(Query halted: Field "notthere" does not exist.)
    end

    test "prefixes classified Postgrex server messages" do
      error =
        query_error(
          backend: PostgresAdaptor,
          raw_error:
            Postgrex.Error.exception(
              postgres: %{
                code: "42703",
                message: ~s|column l0.nonexistent does not exist|,
                severity: "ERROR"
              }
            )
        )

      assert QueryErrorHelpers.query_halted_message(error) ==
               ~s(Query halted: Field "nonexistent" does not exist.)
    end

    test "does not prefix generic fallback messages" do
      error =
        query_error(
          backend: BigQueryAdaptor,
          raw_error: %RuntimeError{message: "raw backend syntax error"}
        )

      assert QueryErrorHelpers.query_halted_message(error) ==
               QueryErrorHelpers.generic_query_error_message()
    end
  end

  defp query_error(attrs) do
    attrs =
      Keyword.merge(
        [
          code: :invalid_query,
          description: nil
        ],
        attrs
      )

    struct!(QueryError, attrs)
  end
end
