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

  describe "OpenTelemetry schema generation" do
    test "OTel: start_time and end_time are TIMESTAMP" do
      otel_trace_params = %{
        "resource" => %{"service.name" => "my-service"},
        "scope" => %{"name" => "my-scope", "version" => "1.0"},
        "attributes" => %{"http.status" => 200},
        "start_time" => System.os_time(:nanosecond),
        "end_time" => System.os_time(:nanosecond),
        "span_id" => "def456",
        "trace_id" => "abc123"
      }

      schema = SchemaBuilder.build_table_schema(otel_trace_params, @default_schema)

      assert %TFS{name: "start_time", type: "TIMESTAMP", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(schema, "start_time")

      assert %TFS{name: "end_time", type: "TIMESTAMP", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(schema, "end_time")
    end

    test "only OTel data converts start_time/end_time to TIMESTAMP, other integers remain INTEGER" do
      # Non-OTel: start_time/end_time stay as INTEGER
      non_otel_params = %{
        "user_id" => "123",
        "start_time" => 1_234_567_890_123_456_789,
        "end_time" => 1_234_567_890_123_456_789,
        "duration" => 109
      }

      non_otel_schema = SchemaBuilder.build_table_schema(non_otel_params, @default_schema)

      assert %TFS{name: "start_time", type: "INTEGER", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(non_otel_schema, "start_time")

      assert %TFS{name: "end_time", type: "INTEGER", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(non_otel_schema, "end_time")

      # OTel: start_time becomes TIMESTAMP, other integer fields stay INTEGER
      otel_params = %{
        "resource" => %{"service.name" => "my-service"},
        "scope" => %{"name" => "my-scope", "version" => "1.0"},
        "attributes" => %{"request_id" => "abc"},
        "start_time" => 1_234_567_890_123_456_789,
        "severity_number" => 9,
        "retry_count" => 3
      }

      otel_schema = SchemaBuilder.build_table_schema(otel_params, @default_schema)

      assert %TFS{name: "start_time", type: "TIMESTAMP", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(otel_schema, "start_time")

      assert %TFS{name: "severity_number", type: "INTEGER", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(otel_schema, "severity_number")

      assert %TFS{name: "retry_count", type: "INTEGER", mode: "NULLABLE"} =
               TestUtils.get_bq_field_schema(otel_schema, "retry_count")
    end
  end
end
