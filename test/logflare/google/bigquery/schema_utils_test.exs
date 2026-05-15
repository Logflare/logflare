defmodule Logflare.Google.BigQuery.SchemaUtilsTest do
  use ExUnit.Case, async: true
  use ExUnitProperties

  alias Logflare.Google.BigQuery.SchemaUtils

  describe "to_typemap/1" do
    test "nil returns nil" do
      assert SchemaUtils.to_typemap(nil) == nil
    end

    test "binary metadata keys are preserved as binaries" do
      assert SchemaUtils.to_typemap(%{"name" => "x", "count" => 1}) == %{
               "name" => %{t: :string},
               "count" => %{t: :integer}
             }
    end

    test "atom metadata keys are converted to binaries" do
      assert SchemaUtils.to_typemap(%{name: "x", count: 1}) == %{
               "name" => %{t: :string},
               "count" => %{t: :integer}
             }
    end

    test "nested map keys are binaries at every level" do
      typemap = SchemaUtils.to_typemap(%{"user" => %{"address" => %{"city" => "Dublin"}}})

      assert typemap == %{
               "user" => %{
                 t: :map,
                 fields: %{
                   "address" => %{
                     t: :map,
                     fields: %{"city" => %{t: :string}}
                   }
                 }
               }
             }
    end

    test "Latin1-encoded key (invalid UTF-8) is normalized to a valid UTF-8 binary" do
      latin1_key =
        <<95, 67, 195, 95, 100, 105, 103, 111, 95, 100, 101, 95, 82, 97, 115, 116, 114, 101, 105,
          111>>

      refute String.valid?(latin1_key)
      typemap = SchemaUtils.to_typemap(%{latin1_key => "x"})
      [normalized_key] = Map.keys(typemap)
      assert String.valid?(normalized_key)
      assert typemap[normalized_key] == %{t: :string}
    end

    test "arbitrary bytes fall back to Latin1 and produce a valid UTF-8 binary" do
      raw = <<0xFF, 0xFE, 0xFD>>
      refute String.valid?(raw)
      typemap = SchemaUtils.to_typemap(%{raw => "x"})
      [normalized_key] = Map.keys(typemap)
      assert String.valid?(normalized_key)
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
end
