defmodule Logflare.Sql.DialectTransformer.ClickhouseTest do
  use Logflare.DataCase

  alias Logflare.Sql.DialectTransformer.Clickhouse
  alias Logflare.Backends.Adaptor.ClickhouseAdaptor

  describe "quote_style/0" do
    test "returns nil for ClickHouse" do
      assert Clickhouse.quote_style() == nil
    end
  end

  describe "dialect/0" do
    test "returns clickhouse string" do
      assert Clickhouse.dialect() == "clickhouse"
    end
  end

  describe "transform_source_name/2" do
    test "delegates to ClickhouseAdaptor.clickhouse_ingest_table_name/1" do
      user = build(:user)
      source = build(:source, name: "test_source", user: user)

      data = %{
        sources: [source],
        dialect: "clickhouse"
      }

      result = Clickhouse.transform_source_name("test_source", data)
      expected = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

      assert result == expected
    end

    test "finds correct source by name from multiple sources" do
      user = build(:user)
      source1 = build(:source, name: "source_one", user: user)
      source2 = build(:source, name: "source_two", user: user)

      data = %{
        sources: [source1, source2],
        dialect: "clickhouse"
      }

      result = Clickhouse.transform_source_name("source_two", data)
      expected = ClickhouseAdaptor.clickhouse_ingest_table_name(source2)

      assert result == expected
    end

    test "handles source with special characters in token" do
      user = build(:user)
      source = build(:source, name: "special_source", user: user)

      data = %{
        sources: [source],
        dialect: "clickhouse"
      }

      result = Clickhouse.transform_source_name("special_source", data)
      expected = ClickhouseAdaptor.clickhouse_ingest_table_name(source)

      assert result == expected
    end
  end

  describe "build_transformation_data/2" do
    test "passes through base data unchanged" do
      user = build(:user)

      base_data = %{
        sources: [],
        dialect: "clickhouse",
        ast: [],
        sandboxed_query: nil
      }

      result = Clickhouse.build_transformation_data(user, base_data)

      assert result == base_data
    end
  end
end
