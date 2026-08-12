defmodule Logflare.Sql.DialectTransformer.ClickHouseTest do
  use Logflare.DataCase

  alias Logflare.Sql.DialectTransformer.ClickHouse

  describe "quote_style/0" do
    test "returns nil for ClickHouse" do
      assert ClickHouse.quote_style() == nil
    end
  end

  describe "dialect/0" do
    test "returns clickhouse string" do
      assert ClickHouse.dialect() == "clickhouse"
    end
  end

  describe "transform_source_name/2" do
    test "passes through the source name unchanged" do
      data = %{sources: [], dialect: "clickhouse"}

      assert ClickHouse.transform_source_name("my_table", data) == "my_table"
    end
  end

  describe "apply_limit/2" do
    test "adds a top-level limit" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT 10"} =
               ClickHouse.apply_limit("SELECT number FROM numbers(100)", 10)
    end

    test "preserves a stricter existing limit and its offset" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT least(5, 10) OFFSET 2"} =
               ClickHouse.apply_limit(
                 "SELECT number FROM numbers(100) LIMIT 5 OFFSET 2",
                 10
               )
    end

    test "caps a larger existing limit" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT least(50, 10)"} =
               ClickHouse.apply_limit("SELECT number FROM numbers(100) LIMIT 50", 10)
    end

    test "places the limit before query-level settings" do
      assert {:ok, "SELECT number FROM numbers(100) LIMIT 10 SETTINGS max_threads = 1"} =
               ClickHouse.apply_limit(
                 "SELECT number FROM numbers(100) SETTINGS max_threads = 1",
                 10
               )
    end

    test "rejects multiple statements" do
      assert {:error, "Expected one ClickHouse query"} =
               ClickHouse.apply_limit("SELECT 1; SELECT 2", 10)
    end
  end

  describe "build_transformation_data/2" do
    test "passes through base data unchanged" do
      user = insert(:user)

      base_data = %{
        sources: [],
        dialect: "clickhouse",
        ast: [],
        sandboxed_query: nil
      }

      result = ClickHouse.build_transformation_data(user, base_data)

      assert result == base_data
    end
  end
end
