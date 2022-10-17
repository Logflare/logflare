defmodule Logflare.Google.BigQuery.SourceSchemaBuilderTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Logflare.TestUtils
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  # doctest SchemaBuilder

  describe "schema builder" do
    test "build_table_schema/1 @list(map) of depth 1" do
      tfs =
        SchemaBuilder.build_fields_schemas([
          %{"string1" => "string1val"},
          %{"string2" => "string1val"}
        ])

      assert tfs == [
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "NULLABLE",
                 name: "string1",
                 type: "STRING"
               },
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "NULLABLE",
                 name: "string2",
                 type: "STRING"
               }
             ]
    end

    test "build_table_schema/1 @list(map) of depth 2" do
      tfs =
        SchemaBuilder.build_fields_schemas([
          %{
            "string1" => "string1val",
            "map_lvl_2" => %{
              "string3" => "string"
            }
          },
          %{
            "string2" => "string1val",
            "map_lvl_2" => %{
              "string4" => "string"
            }
          }
        ])

      expected = [
        %TFS{
          description: nil,
          mode: "REPEATED",
          name: "map_lvl_2",
          type: "RECORD",
          fields: [
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "string3",
              type: "STRING"
            },
            %TFS{
              description: nil,
              fields: nil,
              mode: "NULLABLE",
              name: "string4",
              type: "STRING"
            }
          ]
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "string1",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "string2",
          type: "STRING"
        }
      ]

      TestUtils.BigQuery.assert_equal_schemas(tfs, expected)
    end

    test "build_fields_schema/1 @list(String) of depth 1" do
      tfs =
        SchemaBuilder.build_fields_schemas([
          %{"string1" => ["string", "string"]},
          %{"string2" => ["string1", "string2"]}
        ])

      assert tfs == [
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "REPEATED",
                 name: "string1",
                 type: "STRING"
               },
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "REPEATED",
                 name: "string2",
                 type: "STRING"
               }
             ]
    end

    test "build_fields_schema/1 @list(String) with empty containers" do
      tfs =
        SchemaBuilder.build_fields_schemas([
          %{"string1" => ["string", "string"]},
          %{"string2" => []}
        ])

      assert tfs == [
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "REPEATED",
                 name: "string1",
                 type: "STRING"
               }
             ]

      tfs =
        SchemaBuilder.build_fields_schemas([
          %{"string1" => ["string", "string"]},
          %{"string2" => %{}}
        ])

      assert tfs == [
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "REPEATED",
                 name: "string1",
                 type: "STRING"
               }
             ]

      tfs =
        SchemaBuilder.build_fields_schemas([
          %{"string1" => ["string", "string"]},
          %{"string2" => [[]]}
        ])

      assert tfs == [
               %TFS{
                 description: nil,
                 fields: nil,
                 mode: "REPEATED",
                 name: "string1",
                 type: "STRING"
               }
             ]
    end

    test "build_fields_schema/1 @list(integer|float) of depth 1" do
      tfs =
        SchemaBuilder.build_fields_schemas([
          %{"field1" => [1, 2]},
          %{"field2" => [1.0, 2.0]}
        ])

      assert tfs == [
               %TFS{
                 description: nil,
                 fields: nil,
                 name: "field1",
                 mode: "REPEATED",
                 type: "INTEGER"
               },
               %TFS{
                 description: nil,
                 fields: nil,
                 name: "field2",
                 mode: "REPEATED",
                 type: "FLOAT"
               }
             ]
    end
  end

  describe "integration" do
    test "correctly builds schema from first params metadata" do
      new =
        TestUtils.BigQuery.metadatas().first
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().initial)

      expected = TestUtils.BigQuery.schemas().first

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "correctly builds schema from second params metadata" do
      new =
        TestUtils.BigQuery.metadatas().second
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().first)

      expected = TestUtils.BigQuery.schemas().second

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "correctly builds schemas for metadata with deeply nested keys removed" do
      new =
        TestUtils.BigQuery.metadatas().third_deep_nested_removed
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().second)

      expected = TestUtils.BigQuery.schemas().second

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "correctly builds schema from third params metadata" do
      new =
        TestUtils.BigQuery.metadatas().third
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().second)

      expected = TestUtils.BigQuery.schemas().third

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "correctly builds schema for lists of maps" do
      new =
        TestUtils.BigQuery.metadatas().list_of_maps
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().initial)

      expected = TestUtils.BigQuery.schemas().list_of_maps

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "correctly builds schema for lists of maps with various shapes" do
      %{schema: expected, metadata: metadata} =
        TestUtils.BigQuery.schema_and_payload_metadata(:list_of_maps_of_varying_shapes)

      new = SchemaBuilder.build_table_schema(metadata, TestUtils.BigQuery.schemas().initial)

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end
  end

  describe "schema update" do
    test "schema builder errors on " do
      fun = fn ->
        SchemaBuilder.build_table_schema(
          %{
            "string1" => [
              %{
                "nested_string" => "string",
                "nested_string2" => "string"
              }
            ]
          },
          existing()
        )
      end

      assert catch_error(fun.()) == %Protocol.UndefinedError{
               description: "",
               protocol: Enumerable,
               value: nil
             }
    end
  end

  def existing() do
    %TS{
      fields: [
        %TFS{
          description: nil,
          fields: nil,
          mode: "REQUIRED",
          name: "timestamp",
          type: "TIMESTAMP"
        },
        %TFS{
          description: nil,
          fields: nil,
          mode: "NULLABLE",
          name: "event_message",
          type: "STRING"
        },
        %TFS{
          description: nil,
          fields: [
            %TFS{
              description: nil,
              mode: "NULLABLE",
              name: "string1",
              type: "STRING",
              fields: nil
            }
          ],
          mode: "NULLABLE",
          name: "metadata",
          type: "RECORD"
        }
      ]
    }
  end
end
