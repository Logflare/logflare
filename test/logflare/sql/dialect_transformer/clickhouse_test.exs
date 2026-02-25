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
