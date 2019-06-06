defmodule Logflare.Google.BigQuery.SourceSchemaBuilderTest do
  @moduledoc false
  import Logflare.Source.BigQuery.SchemaBuilder
  import Logflare.Google.BigQuery.TestUtils
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  use ExUnit.Case, async: true

  describe "schema builder" do
    test "build_table_schema/1 @list(map) of depth 1" do
      tfs =
        build_fields_schemas([
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
        build_fields_schemas([
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

      assert_equal_schemas(tfs, expected)
    end
  end

  test "build_fields_schema/1 @list(String) of depth 1" do
    tfs =
      build_fields_schemas([
        %{"string1" => ["string", "string"]},
        %{"string2" => ["string1", "string2"]}
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
end
