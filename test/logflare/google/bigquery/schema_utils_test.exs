defmodule Logflare.Google.BigQuery.SchemaUtilsTest do
  use ExUnit.Case, async: true

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema
  alias GoogleApi.BigQuery.V2.Model.TableSchema
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.TestUtils

  describe "bq_schema_to_flat_typemap/1" do
    test "nil schema returns empty map" do
      assert SchemaUtils.bq_schema_to_flat_typemap(nil) == %{}
    end

    test "scalar leaf fields are dot-keyed with their atom types" do
      flat =
        TestUtils.build_bq_schema(%{"name" => "x", "count" => 1, "active" => true})
        |> SchemaUtils.bq_schema_to_flat_typemap()

      assert flat["name"] == :string
      assert flat["count"] == :integer
      assert flat["active"] == :boolean
    end

    test "nested record fields produce a parent :map entry and child dot-paths" do
      flat =
        TestUtils.build_bq_schema(%{"user" => %{"address" => %{"city" => "Dublin"}}})
        |> SchemaUtils.bq_schema_to_flat_typemap()

      assert flat["user"] == :map
      assert flat["user.address"] == :map
      assert flat["user.address.city"] == :string
    end

    test "repeated scalar fields flatten to a {:list, type} entry" do
      flat =
        TestUtils.build_bq_schema(%{"tags" => ["a", "b"]})
        |> SchemaUtils.bq_schema_to_flat_typemap()

      assert flat["tags"] == {:list, :string}
    end

    test "repeated record fields flatten to parent :map and child dot-paths" do
      flat =
        TestUtils.build_bq_schema(%{"items" => [%{"a" => 1}, %{"b" => 2}]})
        |> SchemaUtils.bq_schema_to_flat_typemap()

      assert flat["items"] == :map
      assert flat["items.a"] == :integer
      assert flat["items.b"] == :integer
    end

    test "nested repeated record fields preserve deeper dot-paths" do
      flat =
        TestUtils.build_bq_schema(%{"items" => [%{"detail" => %{"count" => 1}}]})
        |> SchemaUtils.bq_schema_to_flat_typemap()

      assert flat["items"] == :map
      assert flat["items.detail"] == :map
      assert flat["items.detail.count"] == :integer
    end

    test "manually built BigQuery schemas are flattened without metadata input conversion" do
      schema = %TableSchema{
        fields: [
          %TableFieldSchema{name: "id", type: "STRING", mode: "REQUIRED"},
          %TableFieldSchema{name: "created_at", type: "TIMESTAMP", mode: "NULLABLE"},
          %TableFieldSchema{name: "tags", type: "STRING", mode: "REPEATED"},
          %TableFieldSchema{
            name: "request",
            type: "RECORD",
            mode: "NULLABLE",
            fields: [
              %TableFieldSchema{name: "method", type: "STRING", mode: "NULLABLE"},
              %TableFieldSchema{name: "status", type: "INTEGER", mode: "NULLABLE"}
            ]
          }
        ]
      }

      assert SchemaUtils.bq_schema_to_flat_typemap(schema) == %{
               "id" => :string,
               "created_at" => :datetime,
               "tags" => {:list, :string},
               "request" => :map,
               "request.method" => :string,
               "request.status" => :integer
             }
    end
  end

  describe "get_type_for_path/2" do
    setup do
      flat_schema =
        %{
          "metadata" => %{
            "timestamp" => 1_777_263_766_765_189,
            "count" => 1,
            "items" => [%{"name" => "item"}]
          }
        }
        |> TestUtils.build_bq_schema()
        |> SchemaUtils.bq_schema_to_flat_typemap()

      %{flat_schema: flat_schema}
    end

    test "returns the correct type for paths", %{flat_schema: flat_schema} do
      assert SchemaUtils.get_type_for_path(["metadata", "timestamp"], flat_schema) == :integer
      assert SchemaUtils.get_type_for_path(["metadata", "count"], flat_schema) == :integer
      assert SchemaUtils.get_type_for_path(["metadata", "items", "name"], flat_schema) == :string
    end

    test "handles array indices correctly", %{flat_schema: flat_schema} do
      assert SchemaUtils.get_type_for_path(["metadata", "items", 0, "name"], flat_schema) ==
               :string

      assert SchemaUtils.get_type_for_path(["metadata", "items", "1", "name"], flat_schema) ==
               :string
    end

    test "returns nil for unknown paths or invalid inputs", %{flat_schema: flat_schema} do
      assert SchemaUtils.get_type_for_path(["metadata", "unknown"], flat_schema) == nil
      assert SchemaUtils.get_type_for_path(nil, flat_schema) == nil
      assert SchemaUtils.get_type_for_path(["metadata", "timestamp"], nil) == nil
    end
  end
end
