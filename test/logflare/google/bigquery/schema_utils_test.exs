defmodule Logflare.Google.BigQuery.SchemaUtilsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Google.BigQuery.SchemaUtils
  alias Logflare.TestUtils

  describe "to_typemap/1" do
    test "nil returns nil" do
      assert SchemaUtils.to_typemap(nil) == nil
    end

    test "non-container inputs return %{} instead of raising" do
      assert SchemaUtils.to_typemap("a string") == %{}
      assert SchemaUtils.to_typemap(42) == %{}
      assert SchemaUtils.to_typemap(true) == %{}
      assert SchemaUtils.to_typemap([1, 2, 3]) == %{}
      assert SchemaUtils.to_typemap([]) == %{}
    end

    test "binary keys are atomized" do
      assert SchemaUtils.to_typemap(%{"name" => "x", "count" => 1}) == %{
               name: %{t: :string},
               count: %{t: :integer}
             }
    end

    test "atom keys are preserved" do
      assert SchemaUtils.to_typemap(%{name: "x", count: 1}) == %{
               name: %{t: :string},
               count: %{t: :integer}
             }
    end

    test "Latin1-encoded key (invalid UTF-8) is decoded and atomized" do
      latin1_key =
        <<95, 67, 195, 95, 100, 105, 103, 111, 95, 100, 101, 95, 82, 97, 115, 116, 114, 101, 105,
          111>>

      refute String.valid?(latin1_key)
      typemap = SchemaUtils.to_typemap(%{latin1_key => "x"})
      [normalized_key] = Map.keys(typemap)
      assert is_atom(normalized_key)
      assert String.valid?(Atom.to_string(normalized_key))
      assert typemap[normalized_key] == %{t: :string}
    end

    test "arbitrary high bytes fall back to Latin1 and produce an atom" do
      raw = <<0xFF, 0xFE, 0xFD>>
      refute String.valid?(raw)
      typemap = SchemaUtils.to_typemap(%{raw => "x"})
      [normalized_key] = Map.keys(typemap)
      assert is_atom(normalized_key)
      assert String.valid?(Atom.to_string(normalized_key))
    end

    test "nested map keys are atomized at every level" do
      assert SchemaUtils.to_typemap(%{"user" => %{"address" => %{"city" => "Dublin"}}}) == %{
               user: %{
                 t: :map,
                 fields: %{
                   address: %{
                     t: :map,
                     fields: %{city: %{t: :string}}
                   }
                 }
               }
             }
    end

    test "list of scalar values stays a scalar list type" do
      assert SchemaUtils.to_typemap(%{"items" => ["a", "b"]}) == %{
               items: %{t: {:list, :string}}
             }
    end

    test "homogeneous list of maps merges all map fields into a single REPEATED RECORD" do
      typemap = SchemaUtils.to_typemap(%{"items" => [%{"a" => 1}, %{"b" => 2}]})

      assert typemap == %{
               items: %{
                 t: :map,
                 fields: %{
                   a: %{t: :integer},
                   b: %{t: :integer}
                 }
               }
             }
    end

    test "heterogeneous list-of-maps value skips non-map elements (map at head)" do
      typemap = SchemaUtils.to_typemap(%{"items" => [%{"a" => 1}, "stray", %{"b" => 2}]})

      assert typemap == %{
               items: %{
                 t: :map,
                 fields: %{
                   a: %{t: :integer},
                   b: %{t: :integer}
                 }
               }
             }
    end

    test "heterogeneous list with scalar at head still detects REPEATED RECORD" do
      typemap = SchemaUtils.to_typemap(%{"items" => ["stray", %{"a" => 1}, %{"b" => 2}]})

      assert typemap == %{
               items: %{
                 t: :map,
                 fields: %{
                   a: %{t: :integer},
                   b: %{t: :integer}
                 }
               }
             }
    end

    test "heterogeneous list with a list at head still detects REPEATED RECORD" do
      typemap = SchemaUtils.to_typemap(%{"items" => [[1, 2], %{"a" => 1}, %{"b" => 2}]})

      assert typemap == %{
               items: %{
                 t: :map,
                 fields: %{
                   a: %{t: :integer},
                   b: %{t: :integer}
                 }
               }
             }
    end

    test "list mixing empty maps and non-map elements still surfaces non-empty map fields" do
      typemap = SchemaUtils.to_typemap(%{"items" => [%{}, "stray", %{"a" => 1}]})

      assert typemap.items.fields == %{a: %{t: :integer}}
    end

    test "list with an embedded empty list still detects REPEATED RECORD without crashing" do
      typemap = SchemaUtils.to_typemap(%{"items" => [%{"a" => 1}, [], %{"b" => 2}]})

      assert typemap == %{
               items: %{
                 t: :map,
                 fields: %{
                   a: %{t: :integer},
                   b: %{t: :integer}
                 }
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
