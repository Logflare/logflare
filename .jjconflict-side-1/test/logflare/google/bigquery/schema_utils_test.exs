defmodule Logflare.Google.BigQuery.SchemaUtilsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.TestUtils

  describe "to_typemap/2" do
    test "BigQuery schema field names are preserved as strings" do
      assert %{"user" => %{"address" => %{"city" => "Dublin"}}, "tags" => ["a", "b"]}
             |> TestUtils.build_bq_schema()
             |> SchemaUtils.to_typemap(from: :bigquery_schema) == %{
               "event_message" => %{t: :string},
               "id" => %{t: :string},
               "tags" => %{t: {:list, :string}},
               "timestamp" => %{t: :datetime},
               "user" => %{
                 fields: %{"address" => %{fields: %{"city" => %{t: :string}}, t: :map}},
                 t: :map
               }
             }
    end

    test "atom BigQuery schema field names are converted to strings" do
      assert %{user: %{address: %{city: "Dublin"}}, tags: ["a", "b"]}
             |> TestUtils.build_bq_schema()
             |> SchemaUtils.to_typemap(from: :bigquery_schema) == %{
               "event_message" => %{t: :string},
               "id" => %{t: :string},
               "tags" => %{t: {:list, :string}},
               "timestamp" => %{t: :datetime},
               "user" => %{
                 fields: %{"address" => %{fields: %{"city" => %{t: :string}}, t: :map}},
                 t: :map
               }
             }
    end
  end

  describe "flatten_typemap/1" do
    test "nil and empty map both return empty map" do
      assert SchemaUtils.flatten_typemap(nil) == %{}
      assert SchemaUtils.flatten_typemap(%{}) == %{}
    end

    test "leaf fields at top level (single, multiple, datetime, list-typed)" do
      typemap = %{
        name: %{t: :string},
        count: %{t: :integer},
        active: %{t: :boolean},
        created_at: %{t: :datetime},
        tags: %{t: {:list, :string}}
      }

      assert SchemaUtils.flatten_typemap(typemap) == %{
               "name" => :string,
               "count" => :integer,
               "active" => :boolean,
               "created_at" => :datetime,
               "tags" => {:list, :string}
             }
    end

    test "nested map emits parent :map entry and child leaf entries" do
      typemap = %{
        request: %{
          t: :map,
          fields: %{
            method: %{t: :string},
            status: %{t: :integer}
          }
        }
      }

      assert SchemaUtils.flatten_typemap(typemap) == %{
               "request" => :map,
               "request.method" => :string,
               "request.status" => :integer
             }
    end

    test "deeply nested maps preserve dot-delimited paths" do
      typemap = %{
        request: %{
          t: :map,
          fields: %{
            headers: %{
              t: :map,
              fields: %{
                content_type: %{t: :string}
              }
            }
          }
        }
      }

      assert SchemaUtils.flatten_typemap(typemap) == %{
               "request" => :map,
               "request.headers" => :map,
               "request.headers.content_type" => :string
             }
    end

    test "mixed top-level leaves and nested maps" do
      typemap = %{
        id: %{t: :integer},
        metadata: %{
          t: :map,
          fields: %{
            request_id: %{t: :string}
          }
        }
      }

      assert SchemaUtils.flatten_typemap(typemap) == %{
               "id" => :integer,
               "metadata" => :map,
               "metadata.request_id" => :string
             }
    end

    test "nested list-typed leaf inside a nested map" do
      typemap = %{
        metadata: %{
          t: :map,
          fields: %{
            tags: %{t: {:list, :string}}
          }
        }
      }

      assert SchemaUtils.flatten_typemap(typemap) == %{
               "metadata" => :map,
               "metadata.tags" => {:list, :string}
             }
    end

    property "every input leaf and intermediate-map node appears exactly once in the flat output, under its dot-joined path" do
      check all(typemap <- typemap_generator()) do
        flat = SchemaUtils.flatten_typemap(typemap)
        expected = expected_paths(typemap, "")

        assert flat == expected
      end
    end
  end

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
  end

  defp typemap_generator do
    leaf =
      StreamData.member_of([
        %{t: :string},
        %{t: :integer},
        %{t: :boolean},
        %{t: :float},
        %{t: :datetime},
        %{t: {:list, :string}},
        %{t: {:list, :integer}}
      ])

    key = StreamData.string(:alphanumeric, min_length: 1, max_length: 8)

    typemap =
      StreamData.tree(leaf, fn child ->
        StreamData.map(
          StreamData.map_of(key, child, min_length: 1, max_length: 4),
          fn fields -> %{t: :map, fields: fields} end
        )
      end)

    StreamData.map_of(key, typemap, min_length: 0, max_length: 4)
  end

  defp expected_paths(typemap, prefix) do
    Enum.reduce(typemap, %{}, fn {key, value}, acc ->
      flat_key = if prefix == "", do: to_string(key), else: prefix <> "." <> to_string(key)

      case value do
        %{t: :map, fields: fields} ->
          acc
          |> Map.put(flat_key, :map)
          |> Map.merge(expected_paths(fields, flat_key))

        %{t: type} ->
          Map.put(acc, flat_key, type)
      end
    end)
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
