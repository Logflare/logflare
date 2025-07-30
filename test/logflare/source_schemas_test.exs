defmodule Logflare.SourceSchemasTest do
  use Logflare.DataCase, async: true
  use ExUnitProperties

  alias Logflare.SourceSchemas
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Google.BigQuery.SchemaUtils

  describe "create_source_schema/2" do
    test "with valid attributes returns ok tuple" do
      check all schema <-
                  map_of(
                    string(:utf8),
                    one_of([
                      boolean(),
                      integer(),
                      float(),
                      string(:utf8),
                      map_of(
                        string(:utf8),
                        one_of([boolean(), integer(), float(), string(:utf8)])
                      )
                    ])
                  ),
                schema != %{} do
        user = insert(:user)
        source = insert(:source, user: user)

        bigquery_schema = TestUtils.build_bq_schema(schema)
        schema_flat_map = SchemaUtils.bq_schema_to_flat_typemap(bigquery_schema)

        attrs = %{
          bigquery_schema: bigquery_schema,
          schema_flat_map: schema_flat_map
        }

        assert {:ok, %SourceSchema{} = schema} =
                 SourceSchemas.create_source_schema(source, attrs)

        assert schema.source_id == source.id
        assert schema.bigquery_schema == attrs.bigquery_schema
        assert schema.schema_flat_map == attrs.schema_flat_map
      end
    end

    test "when missing required attributes returns error changeset" do
      user = insert(:user)
      source = insert(:source, user: user)

      assert {:error, %Ecto.Changeset{} = changeset} =
               SourceSchemas.create_source_schema(source, %{})

      assert errors_on(changeset).schema_flat_map == ["can't be blank"]
      assert errors_on(changeset).bigquery_schema == ["can't be blank"]
    end

    test "when schema_flat_map is not flat returns error changeset" do
      user = insert(:user)
      source = insert(:source, user: user)

      attrs = %{
        schema_flat_map: %{"test" => %{"nested" => :string}}
      }

      assert {:error, %Ecto.Changeset{} = changeset} =
               SourceSchemas.create_source_schema(source, attrs)

      assert errors_on(changeset).schema_flat_map == ["must not contain nested maps"]
    end

    test "if source_id was already used it returns error changeset" do
      user = insert(:user)
      source = insert(:source, user: user)
      schema = TestUtils.default_bq_schema()

      insert(:source_schema,
        source: source,
        bigquery_schema: schema,
        schema_flat_map: SchemaUtils.bq_schema_to_flat_typemap(schema)
      )

      attrs = %{
        bigquery_schema: TestUtils.build_bq_schema(%{"test" => 123}),
        schema_flat_map: %{"test" => :integer}
      }

      assert {:error, %Ecto.Changeset{} = changeset} =
               SourceSchemas.create_source_schema(source, attrs)

      assert errors_on(changeset).source_id == ["has already been taken"]
    end
  end

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
