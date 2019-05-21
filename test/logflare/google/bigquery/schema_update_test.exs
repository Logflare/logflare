defmodule Logflare.Google.BigQuery.SchemaUpdateTest do
  @moduledoc false
  use ExUnit.Case
  import Logflare.BigQuery.TableSchemaBuilder
  import Logflare.Google.BigQuery.TestUtils
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  describe "schema update" do
    test "schema builder errors on " do
      fun = fn ->
        build_table_schema(
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

      refute catch_error(fun.()) == %Protocol.UndefinedError{
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
