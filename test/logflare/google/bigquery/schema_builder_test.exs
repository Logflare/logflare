defmodule Logflare.Google.BigQuery.SourceSchemaBuilderTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Logflare.TestUtils
  alias Logflare.Sources.Source.BigQuery.SchemaBuilder
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  @default_schema SchemaBuilder.initial_table_schema()
  doctest SchemaBuilder

  describe "schema diffing" do
    test "schema not updated if keys missing" do
      prev_schema = SchemaBuilder.build_table_schema(%{"a" => %{"b" => 1.0}}, @default_schema)
      curr_schema = SchemaBuilder.build_table_schema(%{"a" => %{}}, prev_schema)

      assert %TFS{name: "b", type: "FLOAT", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(curr_schema, "a.b")

      assert prev_schema == curr_schema
    end

    test "adding new field schemas" do
      prev_schema = SchemaBuilder.build_table_schema(%{"a" => %{"b" => 1.0}}, @default_schema)
      curr_schema = SchemaBuilder.build_table_schema(%{"a" => [%{"c" => 1.0}]}, prev_schema)

      assert %TFS{name: "b", type: "FLOAT", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(curr_schema, "a.b")

      assert %TFS{name: "c", type: "FLOAT", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(curr_schema, "a.c")
    end

    test "highly nested map with map array" do
      schema =
        SchemaBuilder.build_table_schema(
          %{"a" => [%{"b" => %{"c" => [%{"d" => 1.0}]}}]},
          @default_schema
        )

      assert %TFS{name: "d", type: "FLOAT", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(schema, "a.b.c.d")

      for path <- ["a", "a.b", "a.b.c"] do
        [name | _] = String.split(path, ".") |> Enum.reverse()

        assert %TFS{name: ^name, type: "RECORD", mode: "REPEATED"} =
                 TestUtils.get_bq_field_schema(schema, path)
      end
    end

    test "build schema with top-level fields" do
      schema = SchemaBuilder.build_table_schema(%{"a" => "something"}, @default_schema)

      assert %TFS{name: "a", type: "STRING", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(schema, "a")

      schema = SchemaBuilder.build_table_schema(%{"a" => %{"b" => "something"}}, @default_schema)

      assert %TFS{name: "b", type: "STRING", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(schema, "a.b")
    end
  end

  test "schema update: params do not match schema" do
    schema = SchemaBuilder.build_table_schema(%{"a" => %{"b" => 1.0}}, @default_schema)

    for params <- [
          %{"a" => [1.0]},
          %{"a" => ["test"]},
          %{"a" => [%{"b" => %{"c" => 1.0}}]},
          %{"a" => [%{"b" => [%{"c" => 1.0}]}]}
        ] do
      assert_raise Protocol.UndefinedError, fn ->
        SchemaBuilder.build_table_schema(params, schema)
      end
    end
  end
end
