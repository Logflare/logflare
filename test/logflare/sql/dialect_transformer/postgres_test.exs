defmodule Logflare.Sql.DialectTransformer.PostgresTest do
  use Logflare.DataCase

  alias Logflare.Sql.DialectTransformer.Postgres
  alias Logflare.Backends.Adaptor.PostgresAdaptor

  describe "quote_style/0" do
    test "returns double quotes for Postgres" do
      assert Postgres.quote_style() == "\""
    end
  end

  describe "dialect/0" do
    test "returns postgres string" do
      assert Postgres.dialect() == "postgres"
    end
  end

  describe "transform_source_name/2" do
    test "delegates to PostgresAdaptor.table_name/1" do
      user = insert(:user)
      source = insert(:source, name: "test_source", user: user)

      data = %{
        sources: [source],
        dialect: "postgres"
      }

      result = Postgres.transform_source_name("test_source", data)
      expected = PostgresAdaptor.table_name(source)

      assert result == expected
    end

    test "finds correct source by name from multiple sources" do
      user = insert(:user)
      source1 = insert(:source, name: "source_one", user: user)
      source2 = insert(:source, name: "source_two", user: user)

      data = %{
        sources: [source1, source2],
        dialect: "postgres"
      }

      result = Postgres.transform_source_name("source_two", data)
      expected = PostgresAdaptor.table_name(source2)

      assert result == expected
    end

    test "handles source with special characters in token" do
      user = insert(:user)
      source = insert(:source, name: "special_source", user: user)

      data = %{
        sources: [source],
        dialect: "postgres"
      }

      result = Postgres.transform_source_name("special_source", data)
      expected = PostgresAdaptor.table_name(source)

      assert result == expected
    end
  end
end
