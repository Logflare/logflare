defmodule Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchemaTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.QueryTemplates
  alias Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema

  @allowed_types ~w(
    string int long double boolean timestamptz
    map<string,string>
    list<long> list<double> list<string> list<timestamptz> list<map<string,string>>
  )

  @required_field_names ~w(id timestamp)

  describe "fields/1" do
    test "matches ClickHouse's columns_for_type/1 names and order for every event type" do
      for event_type <- IcebergSchema.event_types() do
        field_names = Enum.map(IcebergSchema.fields(event_type), & &1.name)

        assert field_names == QueryTemplates.columns_for_type(event_type)
      end
    end

    test "all field names are lowercase" do
      for event_type <- IcebergSchema.event_types(), field <- IcebergSchema.fields(event_type) do
        assert field.name == String.downcase(field.name)
      end
    end

    test "every field type is in the allowed DSL set" do
      for event_type <- IcebergSchema.event_types(), field <- IcebergSchema.fields(event_type) do
        assert field.type in @allowed_types
      end
    end

    test "every event type has a required timestamp field typed timestamptz" do
      for event_type <- IcebergSchema.event_types() do
        assert %{type: "timestamptz", required: true} =
                 Enum.find(IcebergSchema.fields(event_type), &(&1.name == "timestamp"))
      end
    end

    test "only id and timestamp are required; all other columns are optional" do
      for event_type <- IcebergSchema.event_types(), field <- IcebergSchema.fields(event_type) do
        if field.name in @required_field_names do
          assert field.required
        else
          refute field.required
        end
      end
    end
  end

  test "table_properties/1" do
    for event_type <- IcebergSchema.event_types() do
      properties = IcebergSchema.table_properties(event_type)
      assert %{"logflare.schema-version" => version} = properties
      assert version == IcebergSchema.schema_version(event_type)
      assert %{"commit.retry.total-timeout-ms" => timeout} = properties
      assert timeout =~ ~r/^\d+$/
    end
  end

  test "schema_version/1" do
    for event_type <- IcebergSchema.event_types() do
      version = IcebergSchema.schema_version(event_type)
      assert version =~ ~r/^[0-9a-f]{64}$/
      version
    end
  end

  describe "table_name/1" do
    test "maps each event type to its OTEL table name" do
      assert IcebergSchema.table_name(:log) == "otel_logs"
      assert IcebergSchema.table_name(:metric) == "otel_metrics"
      assert IcebergSchema.table_name(:trace) == "otel_traces"
    end
  end
end
