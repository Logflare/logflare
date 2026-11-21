defmodule Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPCTest do
  use ExUnit.Case, async: true

  alias Explorer.DataFrame
  alias Logflare.Backends.Adaptor.BigQueryAdaptor.ArrowIPC

  @fields [
    {"id", :string, true},
    {"severity_number", :integer, false},
    {"attributes", :string_map, false}
  ]

  describe "get_ipc_bytes_with_schema/3" do
    test "encodes rows against the given schema, ignoring fields not in it" do
      ndjson = """
      {"id":"a","severity_number":9,"attributes":{"name":"bob","user.id":"5"},"extra":1}
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

    test "writes missing maps and arrays as empty lists" do
      fields = @fields ++ [{"names", :string_array, false}]

      assert [%{"attributes" => [], "names" => []}] =
               DataFrame.to_rows(decode!(~s({"id":"a"}\n), fields))
    end

    test "keeps an input below the default limit in one record batch" do
      ndjson = Enum.map_join(1..1_000, &~s({"id":"#{&1}"}\n))
      assert {:ok, _schema, [_batch]} = ArrowIPC.get_ipc_bytes_with_schema(ndjson, @fields, :zstd)
    end

    test "splits rows into record batches bounded by input bytes" do
      rows = for n <- 1..100, do: ~s({"id":"#{String.pad_leading("#{n}", 3, "0")}"}\n)
      row_bytes = byte_size(hd(rows))
      max_batch_bytes = row_bytes * 7

      assert {:ok, schema, batches} =
               ArrowIPC.get_ipc_bytes_with_schema(
                 Enum.join(rows),
                 @fields,
                 :none,
                 max_batch_bytes
               )

      batch_ids = Enum.map(batches, &ids(schema, &1))

      assert length(batches) == 15
      assert Enum.all?(batch_ids, &(length(&1) * row_bytes <= max_batch_bytes))
      assert List.flatten(batch_ids) == Enum.map(1..100, &String.pad_leading("#{&1}", 3, "0"))
    end

    test "gives a row larger than the limit a record batch of its own" do
      long_id = String.duplicate("x", 100)
      ndjson = ~s({"id":"a"}\n{"id":"#{long_id}"}\n{"id":"b"}\n)

      assert {:ok, schema, batches} =
               ArrowIPC.get_ipc_bytes_with_schema(ndjson, @fields, :none, 20)

      assert Enum.map(batches, &ids(schema, &1)) == [["a"], [long_id], ["b"]]
    end

    test "rejects a non-positive batch limit" do
      assert_raise ArgumentError, fn ->
        ArrowIPC.get_ipc_bytes_with_schema(~s({"id":"a"}\n), @fields, :none, 0)
      end
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

    test "returns an error for a row that does not fit the schema" do
      for {ndjson, expected} <- [
            {~s({"severity_number":9}\n), "id"},
            {~s({"id":"a","severity_number":"high"}\n), "severity_number"},
            {"[1]\n", "expected {"}
          ] do
        assert {:error, message} = ArrowIPC.get_ipc_bytes_with_schema(ndjson, @fields, :none)
        assert message =~ expected
      end
    end

    test "fails the batch for values the mapper never produces" do
      for {field, json} <- [
            {{"c", :string, false}, ~s({"c":5})},
            {{"c", :string, false}, ~s({"c":true})},
            {{"c", :string_map, false}, ~s({"c":{"k":5}})},
            {{"c", :string_array, false}, ~s({"c":["a",null]})}
          ] do
        assert {:error, _message} =
                 ArrowIPC.get_ipc_bytes_with_schema(json <> "\n", [field], :none)
      end
    end

    test "coerces floats and numeric strings" do
      integer = [{"c", :integer, false}]

      assert [%{"c" => 2}] = DataFrame.to_rows(decode!(~s({"c":2.9}\n), integer))
      assert [%{"c" => 5}] = DataFrame.to_rows(decode!(~s({"c":"5"}\n), integer))

      assert {:ok, _schema, [_batch]} =
               ArrowIPC.get_ipc_bytes_with_schema(
                 ~s({"t":1.5}\n),
                 [{"t", :timestamp, false}],
                 :none
               )
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

    test "matches row keys to columns as is, without renaming" do
      fields = [{"events_name", :string_array, false}]

      assert [%{"events_name" => ["exception"]}] =
               DataFrame.to_rows(decode!(~s({"events_name":["exception"]}\n), fields))

      assert [%{"events_name" => []}] =
               DataFrame.to_rows(decode!(~s({"events.name":["exception"]}\n), fields))
    end

    test "writes json_array string elements as is" do
      fields = [{"events_attributes", :json_array, false}]
      ndjson = ~s({"events_attributes":["{\\"exception.message\\":\\"boom\\"}","{}"]}\n)

      assert [%{"events_attributes" => [~s({"exception.message":"boom"}), "{}"]}] =
               DataFrame.to_rows(decode!(ndjson, fields))
    end

    test "rejects unknown field types, including timestamp arrays" do
      for field_type <- [:uuid, :timestamp_array] do
        assert_raise ArgumentError, fn ->
          ArrowIPC.get_ipc_bytes_with_schema(~s({"c":1}\n), [{"c", field_type, false}], :none)
        end
      end
    end
  end

  defp ids(schema, batch) do
    [schema, batch]
    |> IO.iodata_to_binary()
    |> DataFrame.load_ipc_stream!()
    |> DataFrame.pull("id")
    |> Explorer.Series.to_list()
  end

  defp decode!(ndjson, fields \\ @fields) do
    {:ok, schema, batches} = ArrowIPC.get_ipc_bytes_with_schema(ndjson, fields, :none)
    DataFrame.load_ipc_stream!(IO.iodata_to_binary([schema | batches]))
  end
end
