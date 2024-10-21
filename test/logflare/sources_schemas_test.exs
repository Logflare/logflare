defmodule Logflare.SourceSchemasTest do
  @moduledoc false
  use Logflare.DataCase

  alias Logflare.SourceSchemas

  describe "format_schema/3" do
    setup do
      insert(:plan, name: "Free")
      user = insert(:user)
      source = insert(:source, user: user)
      %{user: user, source: source}
    end

    test "dot notation with nested values", %{
      source: source
    } do
      schema =
        insert(:source_schema,
          source: source,
          bigquery_schema:
            TestUtils.build_bq_schema(%{
              "test" => %{"nested" => 123, "listical" => ["testing", "123"]}
            })
        )

      assert %{
               "test.nested" => "integer",
               "timestamp" => "datetime",
               "test.listical" => "string[]"
             } = params = SourceSchemas.format_schema(schema, :dot)

      refute Map.get(params, "test")
    end

    test "json schema ", %{
      source: source
    } do
      schema =
        insert(:source_schema,
          source: source,
          bigquery_schema:
            TestUtils.build_bq_schema(%{
              "test" => %{"nested" => 123, "listical" => ["testing", "123"]}
            })
        )

      assert %{
               "properties" => %{
                 "test" => %{
                   "type" => "object",
                   "properties" => %{
                     "nested" => %{
                       "type" => "number"
                     },
                     "listical" => %{
                       "type" => "array",
                       "items" => %{"type" => "string"}
                     }
                   }
                 }
               }
             } = SourceSchemas.format_schema(schema, :json_schema)
    end
  end
end
