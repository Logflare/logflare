defmodule Logflare.SourcesTest do
  use Logflare.DataCase

  alias Logflare.Sources

  describe "source_schemas" do
    alias Logflare.Sources.SourceSchema

    @valid_attrs %{bigquery_schema: "some bigquery_schema"}
    @update_attrs %{bigquery_schema: "some updated bigquery_schema"}
    @invalid_attrs %{bigquery_schema: nil}

    def source_schema_fixture(attrs \\ %{}) do
      {:ok, source_schema} =
        attrs
        |> Enum.into(@valid_attrs)
        |> Sources.create_source_schema()

      source_schema
    end

    test "list_source_schemas/0 returns all source_schemas" do
      source_schema = source_schema_fixture()
      assert Sources.list_source_schemas() == [source_schema]
    end

    test "get_source_schema!/1 returns the source_schema with given id" do
      source_schema = source_schema_fixture()
      assert Sources.get_source_schema!(source_schema.id) == source_schema
    end

    test "create_source_schema/1 with valid data creates a source_schema" do
      assert {:ok, %SourceSchema{} = source_schema} = Sources.create_source_schema(@valid_attrs)
      assert source_schema.bigquery_schema == "some bigquery_schema"
    end

    test "create_source_schema/1 with invalid data returns error changeset" do
      assert {:error, %Ecto.Changeset{}} = Sources.create_source_schema(@invalid_attrs)
    end

    test "update_source_schema/2 with valid data updates the source_schema" do
      source_schema = source_schema_fixture()
      assert {:ok, %SourceSchema{} = source_schema} = Sources.update_source_schema(source_schema, @update_attrs)
      assert source_schema.bigquery_schema == "some updated bigquery_schema"
    end

    test "update_source_schema/2 with invalid data returns error changeset" do
      source_schema = source_schema_fixture()
      assert {:error, %Ecto.Changeset{}} = Sources.update_source_schema(source_schema, @invalid_attrs)
      assert source_schema == Sources.get_source_schema!(source_schema.id)
    end

    test "delete_source_schema/1 deletes the source_schema" do
      source_schema = source_schema_fixture()
      assert {:ok, %SourceSchema{}} = Sources.delete_source_schema(source_schema)
      assert_raise Ecto.NoResultsError, fn -> Sources.get_source_schema!(source_schema.id) end
    end

    test "change_source_schema/1 returns a source_schema changeset" do
      source_schema = source_schema_fixture()
      assert %Ecto.Changeset{} = Sources.change_source_schema(source_schema)
    end
  end
end
