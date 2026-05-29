defmodule Logflare.Google.BigQuery.SchemaUtilsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema
  alias GoogleApi.BigQuery.V2.Model.TableSchema
  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.TestUtils

  describe "to_typemap_from_bigquery_schema/1" do
    test "non-schema inputs return an empty typemap" do
      assert SchemaUtils.to_typemap_from_bigquery_schema(nil) == %{}
      assert SchemaUtils.to_typemap_from_bigquery_schema("a string") == %{}
      assert SchemaUtils.to_typemap_from_bigquery_schema(42) == %{}
      assert SchemaUtils.to_typemap_from_bigquery_schema(true) == %{}
      assert SchemaUtils.to_typemap_from_bigquery_schema([1, 2, 3]) == %{}
      assert SchemaUtils.to_typemap_from_bigquery_schema([]) == %{}
    end
  end

  describe "bq_schema_to_flat_typemap/1" do
    test "nil schema returns empty map" do
      assert SchemaUtils.bq_schema_to_flat_typemap(nil) == %{}
    end

    test "scalar leaf fields are dot-keyed with their atom types" do
      assert build_bq_schema_flat_typemap(%{"name" => "x", "count" => 1, "active" => true}) == %{
               "name" => :string,
               "count" => :integer,
               "active" => :boolean
             }
    end

    test "binary keys are preserved as string paths" do
      assert build_bq_schema_flat_typemap(%{"name" => "x", "count" => 1}) == %{
               "name" => :string,
               "count" => :integer
             }
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
      assert build_bq_schema_flat_typemap(%{"user" => %{"address" => %{"city" => "Dublin"}}}) ==
               %{
                 "user" => :map,
                 "user.address" => :map,
                 "user.address.city" => :string
               }
    end

    test "list of scalar values stays a scalar list type" do
      assert build_bq_schema_flat_typemap(%{"items" => ["a", "b"]}) == %{
               "items" => {:list, :string}
             }
    end

    test "homogeneous list of maps merges all map fields into a repeated record path" do
      assert build_bq_schema_flat_typemap(%{"items" => [%{"a" => 1}, %{"b" => 2}]}) == %{
               "items" => :map,
               "items.a" => :integer,
               "items.b" => :integer
             }
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
      assert build_bq_schema_flat_typemap(%{"items" => [%{"a" => 1}, [], %{"b" => 2}]}) == %{
               "items" => :map,
               "items.a" => :integer,
               "items.b" => :integer
             }
    end

    test "deeply nested maps preserve dot-delimited paths" do
      assert build_bq_schema_flat_typemap(%{
               "request" => %{"headers" => %{"content_type" => "application/json"}}
             }) == %{
               "request" => :map,
               "request.headers" => :map,
               "request.headers.content_type" => :string
             }
    end

    test "mixed top-level leaves and nested maps" do
      assert build_bq_schema_flat_typemap(%{
               "custom_id" => 123,
               "metadata" => %{"request_id" => "abc"}
             }) == %{
               "custom_id" => :integer,
               "metadata" => :map,
               "metadata.request_id" => :string
             }
    end

    test "nested list-typed leaf inside a nested map" do
      assert build_bq_schema_flat_typemap(%{"metadata" => %{"tags" => ["a", "b"]}}) == %{
               "metadata" => :map,
               "metadata.tags" => {:list, :string}
             }
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

    property "generated metadata schemas include every field in the flat map" do
      check all(metadata <- metadata_generator(), max_runs: 50) do
        assert build_bq_schema_flat_typemap(%{"metadata" => metadata}) ==
                 expected_flat_map(metadata, "metadata")
      end
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

  defp build_bq_schema_flat_typemap(input) do
    input
    |> TestUtils.build_bq_schema()
    |> SchemaUtils.bq_schema_to_flat_typemap()
    |> Map.drop(["event_message", "id", "timestamp"])
  end

  defp metadata_generator(depth \\ 2) do
    StreamData.map_of(key_generator(), metadata_value_generator(depth),
      min_length: 1,
      max_length: 4
    )
  end

  defp metadata_value_generator(0) do
    StreamData.one_of([
      scalar_generator(),
      scalar_list_generator()
    ])
  end

  defp metadata_value_generator(depth) do
    StreamData.one_of([
      scalar_generator(),
      scalar_list_generator(),
      metadata_generator(depth - 1),
      StreamData.list_of(metadata_generator(depth - 1), min_length: 1, max_length: 1)
    ])
  end

  defp key_generator do
    first_chars = string_chars(?A..?Z) ++ string_chars(?a..?z) ++ ["_"]
    chars = first_chars ++ string_chars(?0..?9)

    StreamData.map(
      {StreamData.member_of(first_chars),
       StreamData.list_of(StreamData.member_of(chars), max_length: 7)},
      fn {first, rest} -> first <> Enum.join(rest) end
    )
  end

  defp string_chars(range), do: Enum.map(range, &<<&1::utf8>>)

  defp scalar_generator do
    StreamData.one_of([
      StreamData.string(:alphanumeric, min_length: 1, max_length: 12),
      StreamData.integer(),
      StreamData.float(min: -1_000.0, max: 1_000.0),
      StreamData.boolean()
    ])
  end

  defp scalar_list_generator do
    StreamData.one_of([
      StreamData.list_of(StreamData.string(:alphanumeric, min_length: 1, max_length: 12),
        min_length: 1,
        max_length: 4
      ),
      StreamData.list_of(StreamData.integer(), min_length: 1, max_length: 4),
      StreamData.list_of(StreamData.float(min: -1_000.0, max: 1_000.0),
        min_length: 1,
        max_length: 4
      ),
      StreamData.list_of(StreamData.boolean(), min_length: 1, max_length: 4)
    ])
  end

  defp expected_flat_map(map, prefix) do
    Enum.reduce(map, %{prefix => :map}, fn {key, value}, acc ->
      path = prefix <> "." <> key

      Map.merge(acc, expected_flat_entry(value, path))
    end)
  end

  defp expected_flat_entry(value, path) when is_map(value), do: expected_flat_map(value, path)

  defp expected_flat_entry(value, path) when is_list(value) do
    value
    |> Enum.filter(&is_map/1)
    |> expected_flat_entry_from_list(value, path)
  end

  defp expected_flat_entry(value, path), do: %{path => scalar_type(value)}

  defp expected_flat_entry_from_list([], value, path),
    do: %{path => {:list, scalar_type(hd(value))}}

  defp expected_flat_entry_from_list(maps, _value, path) do
    Enum.reduce(maps, %{path => :map}, fn item, acc ->
      Map.merge(acc, expected_flat_map(item, path))
    end)
  end

  defp scalar_type(value) when is_binary(value), do: :string
  defp scalar_type(value) when is_integer(value), do: :integer
  defp scalar_type(value) when is_float(value), do: :float
  defp scalar_type(value) when is_boolean(value), do: :boolean

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
