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
      assert_flat_typemap(%{"name" => "x", "count" => 1, "active" => true}, %{
        "name" => :string,
        "count" => :integer,
        "active" => :boolean
      })
    end

    test "binary keys are preserved as string paths" do
      assert_flat_typemap(%{"name" => "x", "count" => 1}, %{
        "name" => :string,
        "count" => :integer
      })
    end

    test "field names are converted into string paths" do
      schema = %TableSchema{
        fields: [
          %TableFieldSchema{name: "name", type: "STRING", mode: "NULLABLE"},
          %TableFieldSchema{name: "count", type: "INTEGER", mode: "NULLABLE"}
        ]
      }

      assert SchemaUtils.bq_schema_to_flat_typemap(schema) == %{
               "name" => :string,
               "count" => :integer
             }
    end

    test "nested map keys are flattened at every level" do
      assert_flat_typemap(%{"user" => %{"address" => %{"city" => "Dublin"}}}, %{
        "user" => :map,
        "user.address" => :map,
        "user.address.city" => :string
      })
    end

    test "list of scalar values stays a scalar list type" do
      assert_flat_typemap(%{"items" => ["a", "b"]}, %{
        "items" => {:list, :string}
      })
    end

    test "homogeneous list of maps merges all map fields into a repeated record path" do
      assert_flat_typemap(%{"items" => [%{"a" => 1}, %{"b" => 2}]}, %{
        "items" => :map,
        "items.a" => :integer,
        "items.b" => :integer
      })
    end

    test "heterogeneous list-of-maps value skips non-map elements when map is at head" do
      assert SchemaUtils.bq_schema_to_flat_typemap(items_record_schema()) == %{
               "items" => :map,
               "items.a" => :integer,
               "items.b" => :integer
             }
    end

    test "heterogeneous list with scalar at head still detects repeated record paths" do
      assert SchemaUtils.bq_schema_to_flat_typemap(items_record_schema()) == %{
               "items" => :map,
               "items.a" => :integer,
               "items.b" => :integer
             }
    end

    test "heterogeneous list with a list at head still detects repeated record paths" do
      assert SchemaUtils.bq_schema_to_flat_typemap(items_record_schema()) == %{
               "items" => :map,
               "items.a" => :integer,
               "items.b" => :integer
             }
    end

    test "list mixing empty maps and non-map elements still surfaces non-empty map fields" do
      schema = %TableSchema{
        fields: [
          %TableFieldSchema{
            name: "items",
            type: "RECORD",
            mode: "REPEATED",
            fields: [
              %TableFieldSchema{name: "a", type: "INTEGER", mode: "NULLABLE"}
            ]
          }
        ]
      }

      assert SchemaUtils.bq_schema_to_flat_typemap(schema) == %{
               "items" => :map,
               "items.a" => :integer
             }
    end

    test "list with an embedded empty list still detects repeated record paths without crashing" do
      assert_flat_typemap(%{"items" => [%{"a" => 1}, [], %{"b" => 2}]}, %{
        "items" => :map,
        "items.a" => :integer,
        "items.b" => :integer
      })
    end

    test "deeply nested maps preserve dot-delimited paths" do
      assert_flat_typemap(
        %{"request" => %{"headers" => %{"content_type" => "application/json"}}},
        %{
          "request" => :map,
          "request.headers" => :map,
          "request.headers.content_type" => :string
        }
      )
    end

    test "mixed top-level leaves and nested maps" do
      assert_flat_typemap(%{"custom_id" => 123, "metadata" => %{"request_id" => "abc"}}, %{
        "custom_id" => :integer,
        "metadata" => :map,
        "metadata.request_id" => :string
      })
    end

    test "nested list-typed leaf inside a nested map" do
      assert_flat_typemap(%{"metadata" => %{"tags" => ["a", "b"]}}, %{
        "metadata" => :map,
        "metadata.tags" => {:list, :string}
      })
    end

    test "manually built BigQuery schemas are flattened" do
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

  defp assert_flat_typemap(input, expected) do
    flat =
      input
      |> TestUtils.build_bq_schema()
      |> SchemaUtils.bq_schema_to_flat_typemap()

    assert Map.take(flat, Map.keys(expected)) == expected
  end

  defp items_record_schema do
    %TableSchema{
      fields: [
        %TableFieldSchema{
          name: "items",
          type: "RECORD",
          mode: "REPEATED",
          fields: [
            %TableFieldSchema{name: "a", type: "INTEGER", mode: "NULLABLE"},
            %TableFieldSchema{name: "b", type: "INTEGER", mode: "NULLABLE"}
          ]
        }
      ]
    }
  end
end
