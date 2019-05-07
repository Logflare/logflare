defmodule Logflare.Google.BigQuery.TableSchemaBuilderTest do
  import Logflare.BigQuery.TableSchemaBuilder
  use ExUnit.Case

  describe "schema builder" do
    @tag run: true
    test "build_table_schema/1 @list(map) of depth 1" do
      tfs =
        build_fields_schemas([
          %{
            "string1" => "string1val"
          },
          %{
            "string2" => "string1val"
          }
        ])

      assert tfs == [
               %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                 description: nil,
                 fields: nil,
                 mode: "NULLABLE",
                 name: "string1",
                 type: "STRING"
               },
               %GoogleApi.BigQuery.V2.Model.TableFieldSchema{
                 description: nil,
                 fields: nil,
                 mode: "NULLABLE",
                 name: "string2",
                 type: "STRING"
               }
             ]
    end
  end
end
