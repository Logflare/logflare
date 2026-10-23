defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPCTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC

  # timestamp columns are left out: Polars cannot parse the "+00" timezone BigQuery expects
  @fields [
    {"id", :string, true},
    {"severity_number", :integer, false},
    {"attributes", :string_map, false}
  ]

  describe "get_ipc_bytes_with_schema/3" do
    test "encodes rows against the given schema" do
      ndjson = """
      {"id":"a","severity_number":9,"attributes":{"name":"bob","user.id":"5"}}
      {"id":"b","attributes":{}}
      """

      df = decode!(ndjson)

      assert DataFrame.dtypes(df) == %{
               "id" => :string,
               "severity_number" => {:s, 64},
               "attributes" => {:list, {:struct, [{"key", :string}, {"value", :string}]}}
             }

      assert DataFrame.to_rows(df) == [
               %{
                 "id" => "a",
                 "severity_number" => 9,
                 "attributes" => [
                   %{"key" => "name", "value" => "bob"},
                   %{"key" => "user.id", "value" => "5"}
                 ]
               },
               %{"id" => "b", "severity_number" => nil, "attributes" => []}
             ]
    end

    test "writes a missing attribute map as an empty list" do
      assert [%{"attributes" => []}] = DataFrame.to_rows(decode!(~s({"id":"a"}\n)))
    end

    test "ignores fields that are not in the schema" do
      assert [%{"id" => "a"} = row] = DataFrame.to_rows(decode!(~s({"id":"a","extra":1}\n)))
      refute Map.has_key?(row, "extra")
    end

    test "produces a single record batch" do
      ndjson = Enum.map_join(1..1_000, &~s({"id":"#{&1}"}\n))
      assert {:ok, _schema, [_batch]} = ArrowIPC.get_ipc_bytes_with_schema(ndjson, @fields, :zstd)
    end

    test "encodes timestamps given as Unix microseconds" do
      fields = [{"timestamp", :timestamp, true}]

      assert {:ok, _schema, [_batch]} =
               ArrowIPC.get_ipc_bytes_with_schema(
                 ~s({"timestamp":1761200000123456}\n),
                 fields,
                 :zstd
               )
    end

    test "returns an error when a required field is missing" do
      assert {:error, message} =
               ArrowIPC.get_ipc_bytes_with_schema(~s({"severity_number":9}\n), @fields, :none)

      assert message =~ "id"
    end

    test "returns an error when a value does not fit its column type" do
      assert {:error, message} =
               ArrowIPC.get_ipc_bytes_with_schema(
                 ~s({"id":"a","severity_number":"high"}\n),
                 @fields,
                 :none
               )

      assert message =~ "severity_number"
    end

    test "encodes float, boolean and array columns" do
      fields = [
        {"sum", :float, false},
        {"is_monotonic", :boolean, false},
        {"bucket_counts", :integer_array, false},
        {"explicit_bounds", :float_array, false},
        {"names", :string_array, false}
      ]

      ndjson =
        ~s({"sum":12.5,"is_monotonic":true,"bucket_counts":[1,2],"explicit_bounds":[5.0],"names":["a"]}\n)

      df = decode!(ndjson, fields)

      assert DataFrame.dtypes(df) == %{
               "sum" => {:f, 64},
               "is_monotonic" => :boolean,
               "bucket_counts" => {:list, {:s, 64}},
               "explicit_bounds" => {:list, {:f, 64}},
               "names" => {:list, :string}
             }

      assert DataFrame.to_rows(df) == [
               %{
                 "sum" => 12.5,
                 "is_monotonic" => true,
                 "bucket_counts" => [1, 2],
                 "explicit_bounds" => [5.0],
                 "names" => ["a"]
               }
             ]
    end

    test "writes missing arrays as empty lists" do
      fields = [{"id", :string, true}, {"names", :string_array, false}]

      assert [%{"names" => []}] = DataFrame.to_rows(decode!(~s({"id":"a"}\n), fields))
    end

    test "matches dotted row keys to underscored columns" do
      fields = [{"events_name", :string_array, false}]

      assert [%{"events_name" => ["exception"]}] =
               DataFrame.to_rows(decode!(~s({"events.name":["exception"]}\n), fields))
    end

    test "writes json_array elements as JSON strings" do
      fields = [{"events_attributes", :json_array, false}]
      ndjson = ~s({"events.attributes":[{"exception.message":"boom"},{},"raw"]}\n)

      assert [%{"events_attributes" => [object, empty, raw]}] =
               DataFrame.to_rows(decode!(ndjson, fields))

      assert Jason.decode!(object) == %{"exception.message" => "boom"}
      assert empty == "{}"
      assert raw == "raw"
    end

    test "has no timestamp array type" do
      assert_raise ArgumentError, fn ->
        ArrowIPC.get_ipc_bytes_with_schema(
          ~s({"events_timestamp":[1761200000123456]}\n),
          [{"events_timestamp", :timestamp_array, false}],
          :none
        )
      end
    end

    test "returns an error for a row that is not a JSON object" do
      assert {:error, "row is not a JSON object"} =
               ArrowIPC.get_ipc_bytes_with_schema("[1]\n", @fields, :none)
    end

    test "rejects unknown field types" do
      assert_raise ArgumentError, fn ->
        ArrowIPC.get_ipc_bytes_with_schema(~s({"id":"a"}\n), [{"id", :uuid, true}], :none)
      end
    end
  end

  defp decode!(ndjson, fields \\ @fields) do
    {:ok, schema, batches} = ArrowIPC.get_ipc_bytes_with_schema(ndjson, fields, :none)
    DataFrame.load_ipc_stream!(IO.iodata_to_binary([schema | batches]))
  end
end
