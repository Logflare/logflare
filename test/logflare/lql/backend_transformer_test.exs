defmodule Logflare.Lql.BackendTransformerTest do
  use ExUnit.Case, async: true

  alias Logflare.Lql.BackendTransformer
  alias Logflare.Lql.BackendTransformer.BigQuery

  describe "for_dialect/1" do
    test "returns BigQuery transformer for 'bigquery' string" do
      assert BackendTransformer.for_dialect("bigquery") == BigQuery
    end

    test "returns BigQuery transformer for :bigquery atom" do
      assert BackendTransformer.for_dialect(:bigquery) == BigQuery
    end

    test "raises FunctionClauseError for unknown dialect" do
      assert_raise FunctionClauseError, fn ->
        BackendTransformer.for_dialect("unknown")
      end
    end
  end

  describe "to_dialect/1" do
    test "converts atoms to dialect strings" do
      assert BackendTransformer.to_dialect(:bigquery) == "bigquery"
    end
  end
end
