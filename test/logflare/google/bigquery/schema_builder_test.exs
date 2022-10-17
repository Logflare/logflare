defmodule Logflare.Google.BigQuery.SourceSchemaBuilderTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Logflare.TestUtils
  alias Logflare.Source.BigQuery.SchemaBuilder
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  @default_schema TestUtils.BigQuery.schemas().initial
  doctest SchemaBuilder

  describe "integration" do
    test "schema not updated if keys missing: correctly builds schemas for metadata with deeply nested keys removed" do
      new =
        TestUtils.BigQuery.metadatas().third_deep_nested_removed
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().second)

      expected = TestUtils.BigQuery.schemas().second

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "add new schemas: correctly builds schema from third params metadata" do
      new =
        TestUtils.BigQuery.metadatas().third
        |> SchemaBuilder.build_table_schema(TestUtils.BigQuery.schemas().second)

      expected = TestUtils.BigQuery.schemas().third

      assert TestUtils.BigQuery.deep_schema_to_field_names(new) ===
               TestUtils.BigQuery.deep_schema_to_field_names(expected)

      assert new === expected
    end

    test "maps with varying shape: correctly builds schema for lists of maps with various shapes" do
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
