defmodule Logflare.TableBigQuerySchemaBuilderTest do
  @moduledoc false
  alias Logflare.Source.BigQuery.SchemaBuilder, as: SchemaBuilder
  import Logflare.BigQuery.TableSchema.SchemaBuilderHelpers
  use ExUnit.Case

  describe "schema builder" do
    test "correctly builds schema from first params metadata" do
      new =
        metadatas().first
        |> SchemaBuilder.build_table_schema(schemas().initial)

      expected = schemas().first
      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schema from second params metadata" do
      new =
        metadatas().second
        |> SchemaBuilder.build_table_schema(schemas().first)

      expected = schemas().second
      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schemas for metadata with deeply nested keys removed" do
      new =
        metadatas().third_deep_nested_removed
        |> SchemaBuilder.build_table_schema(schemas().second)

      expected = schemas().second
      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schema from third params metadata" do
      new =
        metadatas().third
        |> SchemaBuilder.build_table_schema(schemas().second)

      expected = schemas().third

      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schema for lists of maps" do
      new =
        metadatas().list_of_maps
        |> SchemaBuilder.build_table_schema(schemas().initial)

      expected = schemas().list_of_maps

      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end

    test "correctly builds schema for lists of maps with various shapes" do
      %{schema: expected, metadata: metadata} =
        schema_and_payload_metadata(:list_of_maps_of_varying_shapes)

      new = SchemaBuilder.build_table_schema(metadata, schemas().initial)

      assert deep_schema_to_field_names(new) === deep_schema_to_field_names(expected)
      assert new === expected
    end
  end
end
