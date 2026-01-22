defmodule Logflare.Sql.DialectTransformer.ClickHouseTest do
  use Logflare.DataCase

  alias Logflare.Sql.DialectTransformer.ClickHouse
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor

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
    test "uses backend token for table name" do
      user = insert(:user)
      source = insert(:source, name: "test_source", user: user)
      backend = insert(:backend, type: :clickhouse, user: user, sources: [source])

      data = %{
        sources: [source],
        dialect: "clickhouse"
      }

      result = ClickHouse.transform_source_name("test_source", data)
      expected = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      assert result == expected
    end

    test "finds correct source by name from multiple sources" do
      user = insert(:user)
      source1 = insert(:source, name: "source_one", user: user)
      source2 = insert(:source, name: "source_two", user: user)
      backend = insert(:backend, type: :clickhouse, user: user, sources: [source1, source2])

      data = %{
        sources: [source1, source2],
        dialect: "clickhouse"
      }

      # Both sources use the same backend, so they share the same table
      result = ClickHouse.transform_source_name("source_two", data)
      expected = ClickHouseAdaptor.clickhouse_ingest_table_name(backend)

      assert result == expected
    end

    test "raises error when source has no ClickHouse backend" do
      user = insert(:user)
      source = insert(:source, name: "no_backend_source", user: user)

      data = %{
        sources: [source],
        dialect: "clickhouse"
      }

      assert_raise RuntimeError, ~r/No ClickHouse backend found for source/, fn ->
        ClickHouse.transform_source_name("no_backend_source", data)
      end
    end

    test "raises error when source has multiple ClickHouse backends" do
      user = insert(:user)
      source = insert(:source, name: "multi_backend_source", user: user)
      insert(:backend, type: :clickhouse, user: user, sources: [source])
      insert(:backend, type: :clickhouse, user: user, sources: [source])

      data = %{
        sources: [source],
        dialect: "clickhouse"
      }

      assert_raise RuntimeError, ~r/Multiple ClickHouse backends found for source/, fn ->
        ClickHouse.transform_source_name("multi_backend_source", data)
      end
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
