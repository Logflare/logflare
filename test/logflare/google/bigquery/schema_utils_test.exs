defmodule Logflare.Google.BigQuery.SchemaUtilsTest do
  use ExUnit.Case, async: true

  alias Logflare.Google.BigQuery.SchemaUtils

  describe "flatten_typemap/1" do
    test "nil input returns empty map" do
      assert SchemaUtils.flatten_typemap(nil) == %{}
    end

    test "empty map returns empty map" do
      assert SchemaUtils.flatten_typemap(%{}) == %{}
    end

    test "single leaf field" do
      assert SchemaUtils.flatten_typemap(%{name: %{t: :string}}) == %{"name" => :string}
    end

    test "multiple leaf fields at top level" do
      typemap = %{
        name: %{t: :string},
        count: %{t: :integer},
        active: %{t: :boolean}
      }

      assert SchemaUtils.flatten_typemap(typemap) == %{
               "name" => :string,
               "count" => :integer,
               "active" => :boolean
             }
    end

    test "datetime leaf" do
      assert SchemaUtils.flatten_typemap(%{created_at: %{t: :datetime}}) == %{
               "created_at" => :datetime
             }
    end

    test "list-typed leaf preserves the tuple value" do
      assert SchemaUtils.flatten_typemap(%{tags: %{t: {:list, :string}}}) == %{
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
  end

  describe "bq_schema_to_flat_typemap/1" do
    test "nil schema returns empty map" do
      assert SchemaUtils.bq_schema_to_flat_typemap(nil) == %{}
    end
  end
end
