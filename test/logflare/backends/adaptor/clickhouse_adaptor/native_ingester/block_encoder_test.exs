defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoderTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.BlockEncoder
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

  # Use the current protocol revision for all tests
  @rev Protocol.dbms_tcp_protocol_version()

  # ---------------------------------------------------------------------------
  # Empty block
  # ---------------------------------------------------------------------------

  describe "encode_empty_block/1" do
    test "produces expected byte sequence" do
      result = BlockEncoder.encode_empty_block(@rev) |> IO.iodata_to_binary()

      # Manually build expected: packet_type(2) + temp_table("") + block_info + 0 cols + 0 rows
      expected =
        IO.iodata_to_binary([
          Protocol.encode_varuint(2),
          Protocol.encode_string(""),
          # block info: {1, UInt8(0)}, {2, Int32(-1)}, {0}
          Protocol.encode_varuint(1),
          Protocol.encode_uint8(0),
          Protocol.encode_varuint(2),
          Protocol.encode_int32(-1),
          Protocol.encode_varuint(0),
          # 0 columns, 0 rows
          Protocol.encode_varuint(0),
          Protocol.encode_varuint(0)
        ])

      assert result == expected
    end

    test "starts with client_data packet type (2)" do
      result = BlockEncoder.encode_empty_block(@rev) |> IO.iodata_to_binary()
      {packet_type, _rest} = Protocol.decode_varuint(result)
      assert packet_type == 2
    end
  end

  # ---------------------------------------------------------------------------
  # Single UInt64 column
  # ---------------------------------------------------------------------------

  describe "encode_data_block/2 with single UInt64 column" do
    test "encodes 3 rows correctly" do
      columns = [{"id", "UInt64", [1, 2, 3]}]
      result = BlockEncoder.encode_data_block(columns, @rev) |> IO.iodata_to_binary()

      # Parse the packet
      {packet_type, rest} = Protocol.decode_varuint(result)
      assert packet_type == 2

      {temp_table, rest} = Protocol.decode_string(rest)
      assert temp_table == ""

      # Skip block info
      rest = skip_block_info(rest)

      {num_columns, rest} = Protocol.decode_varuint(rest)
      assert num_columns == 1

      {num_rows, rest} = Protocol.decode_varuint(rest)
      assert num_rows == 3

      # Column header
      {col_name, rest} = Protocol.decode_string(rest)
      assert col_name == "id"
      {col_type, rest} = Protocol.decode_string(rest)
      assert col_type == "UInt64"

      # Custom serialization flag (rev >= 54454 and rows > 0)
      {custom_ser, rest} = Protocol.decode_uint8(rest)
      assert custom_ser == 0

      # 3 UInt64 values = 24 bytes
      {val1, rest} = Protocol.decode_uint64(rest)
      {val2, rest} = Protocol.decode_uint64(rest)
      {val3, rest} = Protocol.decode_uint64(rest)

      assert val1 == 1
      assert val2 == 2
      assert val3 == 3
      assert rest == <<>>
    end
  end

  # ---------------------------------------------------------------------------
  # Single String column
  # ---------------------------------------------------------------------------

  describe "encode_data_block/2 with String column" do
    test "encodes variable-length strings" do
      columns = [{"name", "String", ["hello", "world", ""]}]
      result = BlockEncoder.encode_data_block(columns, @rev) |> IO.iodata_to_binary()

      # Skip header: packet_type + temp_table + block_info + num_cols + num_rows
      rest = skip_data_block_header(result)

      # Column header
      {col_name, rest} = Protocol.decode_string(rest)
      assert col_name == "name"
      {col_type, rest} = Protocol.decode_string(rest)
      assert col_type == "String"

      # Custom serialization flag
      {0, rest} = Protocol.decode_uint8(rest)

      # Values
      {v1, rest} = Protocol.decode_string(rest)
      {v2, rest} = Protocol.decode_string(rest)
      {v3, rest} = Protocol.decode_string(rest)

      assert v1 == "hello"
      assert v2 == "world"
      assert v3 == ""
      assert rest == <<>>
    end
  end

  # ---------------------------------------------------------------------------
  # Multi-column block
  # ---------------------------------------------------------------------------

  describe "encode_data_block/2 with multiple columns" do
    test "encodes UInt64 + String + Float64" do
      columns = [
        {"id", "UInt64", [10, 20]},
        {"name", "String", ["alice", "bob"]},
        {"score", "Float64", [99.5, 88.0]}
      ]

      result = BlockEncoder.encode_data_block(columns, @rev) |> IO.iodata_to_binary()

      rest = skip_data_block_header(result)

      # Column 1: id UInt64
      {name, rest} = Protocol.decode_string(rest)
      assert name == "id"
      {type, rest} = Protocol.decode_string(rest)
      assert type == "UInt64"
      {0, rest} = Protocol.decode_uint8(rest)
      {v1, rest} = Protocol.decode_uint64(rest)
      {v2, rest} = Protocol.decode_uint64(rest)
      assert v1 == 10
      assert v2 == 20

      # Column 2: name String
      {name, rest} = Protocol.decode_string(rest)
      assert name == "name"
      {type, rest} = Protocol.decode_string(rest)
      assert type == "String"
      {0, rest} = Protocol.decode_uint8(rest)
      {v1, rest} = Protocol.decode_string(rest)
      {v2, rest} = Protocol.decode_string(rest)
      assert v1 == "alice"
      assert v2 == "bob"

      # Column 3: score Float64
      {name, rest} = Protocol.decode_string(rest)
      assert name == "score"
      {type, rest} = Protocol.decode_string(rest)
      assert type == "Float64"
      {0, rest} = Protocol.decode_uint8(rest)
      {v1, rest} = Protocol.decode_float64(rest)
      {v2, rest} = Protocol.decode_float64(rest)
      assert v1 == 99.5
      assert v2 == 88.0
      assert rest == <<>>
    end
  end

  # ---------------------------------------------------------------------------
  # Each column type — encode and decode round-trip
  # ---------------------------------------------------------------------------

  describe "encode_column_values/2" do
    test "UInt8" do
      encoded = BlockEncoder.encode_column_values("UInt8", [0, 127, 255]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 3
      {0, rest} = Protocol.decode_uint8(encoded)
      {127, rest} = Protocol.decode_uint8(rest)
      {255, <<>>} = Protocol.decode_uint8(rest)
    end

    test "UInt16" do
      encoded = BlockEncoder.encode_column_values("UInt16", [0, 65_535]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 4
      {0, rest} = Protocol.decode_uint16(encoded)
      {65_535, <<>>} = Protocol.decode_uint16(rest)
    end

    test "UInt32" do
      encoded =
        BlockEncoder.encode_column_values("UInt32", [0, 0xFFFF_FFFF]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {0, rest} = Protocol.decode_uint32(encoded)
      {0xFFFF_FFFF, <<>>} = Protocol.decode_uint32(rest)
    end

    test "UInt64" do
      encoded =
        BlockEncoder.encode_column_values("UInt64", [0, 0xFFFF_FFFF_FFFF_FFFF])
        |> IO.iodata_to_binary()

      assert byte_size(encoded) == 16
      {0, rest} = Protocol.decode_uint64(encoded)
      {0xFFFF_FFFF_FFFF_FFFF, <<>>} = Protocol.decode_uint64(rest)
    end

    test "Int8" do
      encoded =
        BlockEncoder.encode_column_values("Int8", [-128, 0, 127]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 3
      {-128, rest} = Protocol.decode_int8(encoded)
      {0, rest} = Protocol.decode_int8(rest)
      {127, <<>>} = Protocol.decode_int8(rest)
    end

    test "Int16" do
      encoded =
        BlockEncoder.encode_column_values("Int16", [-32_768, 32_767]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 4
      {-32_768, rest} = Protocol.decode_int16(encoded)
      {32_767, <<>>} = Protocol.decode_int16(rest)
    end

    test "Int32" do
      encoded =
        BlockEncoder.encode_column_values("Int32", [-2_147_483_648, 2_147_483_647])
        |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {-2_147_483_648, rest} = Protocol.decode_int32(encoded)
      {2_147_483_647, <<>>} = Protocol.decode_int32(rest)
    end

    test "Int64" do
      min = -9_223_372_036_854_775_808
      max = 9_223_372_036_854_775_807
      encoded = BlockEncoder.encode_column_values("Int64", [min, max]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 16
      {^min, rest} = Protocol.decode_int64(encoded)
      {^max, <<>>} = Protocol.decode_int64(rest)
    end

    test "Float32" do
      encoded =
        BlockEncoder.encode_column_values("Float32", [0.0, 3.14]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {v1, rest} = Protocol.decode_float32(encoded)
      {v2, <<>>} = Protocol.decode_float32(rest)
      assert_in_delta v1, 0.0, 0.001
      assert_in_delta v2, 3.14, 0.001
    end

    test "Float64" do
      encoded =
        BlockEncoder.encode_column_values("Float64", [0.0, 1.0e308]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 16
      {+0.0, rest} = Protocol.decode_float64(encoded)
      {1.0e308, <<>>} = Protocol.decode_float64(rest)
    end

    test "String with variable lengths" do
      encoded =
        BlockEncoder.encode_column_values("String", ["", "a", "hello world"])
        |> IO.iodata_to_binary()

      {"", rest} = Protocol.decode_string(encoded)
      {"a", rest} = Protocol.decode_string(rest)
      {"hello world", <<>>} = Protocol.decode_string(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # Bool column
  # ---------------------------------------------------------------------------

  describe "Bool column" do
    test "encodes true/false values" do
      encoded =
        BlockEncoder.encode_column_values("Bool", [true, false, true]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 3
      {true, rest} = Protocol.decode_bool(encoded)
      {false, rest} = Protocol.decode_bool(rest)
      {true, <<>>} = Protocol.decode_bool(rest)
    end

    test "in a full data block" do
      columns = [{"active", "Bool", [true, false]}]
      result = BlockEncoder.encode_data_block(columns, @rev) |> IO.iodata_to_binary()
      rest = skip_data_block_header(result)

      {"active", rest} = Protocol.decode_string(rest)
      {"Bool", rest} = Protocol.decode_string(rest)
      {0, rest} = Protocol.decode_uint8(rest)
      {true, rest} = Protocol.decode_bool(rest)
      {false, <<>>} = Protocol.decode_bool(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # DateTime / DateTime64 columns
  # ---------------------------------------------------------------------------

  describe "DateTime column" do
    test "encodes known epoch values" do
      # 2023-11-14 22:13:20 UTC = 1700000000
      values = [0, 1_700_000_000]

      encoded =
        BlockEncoder.encode_column_values("DateTime", values) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {0, rest} = Protocol.decode_datetime(encoded)
      {1_700_000_000, <<>>} = Protocol.decode_datetime(rest)
    end
  end

  describe "DateTime64 column" do
    test "encodes with precision 3 (milliseconds)" do
      # 1700000000000 ms since epoch
      values = [0, 1_700_000_000_000]

      encoded =
        BlockEncoder.encode_column_values("DateTime64(3)", values) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 16
      {0, rest} = Protocol.decode_datetime64(encoded)
      {1_700_000_000_000, <<>>} = Protocol.decode_datetime64(rest)
    end

    test "encodes with precision 9 (nanoseconds)" do
      values = [1_700_000_000_000_000_000]

      encoded =
        BlockEncoder.encode_column_values("DateTime64(9)", values) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {1_700_000_000_000_000_000, <<>>} = Protocol.decode_datetime64(encoded)
    end

    test "encodes with timezone parameter" do
      values = [1_700_000_000_000]

      encoded =
        BlockEncoder.encode_column_values("DateTime64(3, 'UTC')", values)
        |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {1_700_000_000_000, <<>>} = Protocol.decode_datetime64(encoded)
    end
  end

  # ---------------------------------------------------------------------------
  # UUID column
  # ---------------------------------------------------------------------------

  describe "UUID column" do
    test "encodes string UUIDs" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      encoded = BlockEncoder.encode_column_values("UUID", [uuid]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 16

      {decoded_raw, <<>>} = Protocol.decode_uuid(encoded)
      # Re-encode the decoded raw to verify round-trip
      re_encoded = Protocol.encode_uuid(decoded_raw)
      assert re_encoded == Protocol.encode_uuid(uuid)
    end

    test "encodes raw 16-byte UUIDs" do
      raw = <<1::64, 2::64>>
      encoded = BlockEncoder.encode_column_values("UUID", [raw]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 16
      {^raw, <<>>} = Protocol.decode_uuid(encoded)
    end

    test "encodes multiple UUIDs" do
      uuid1 = "550e8400-e29b-41d4-a716-446655440000"
      uuid2 = "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
      encoded = BlockEncoder.encode_column_values("UUID", [uuid1, uuid2]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 32
    end
  end

  # ---------------------------------------------------------------------------
  # FixedString column
  # ---------------------------------------------------------------------------

  describe "FixedString column" do
    test "encodes with null padding" do
      encoded =
        BlockEncoder.encode_column_values("FixedString(4)", ["ab", "abcd"])
        |> IO.iodata_to_binary()

      assert byte_size(encoded) == 8
      {v1, rest} = Protocol.decode_fixed_string(encoded, 4)
      {v2, <<>>} = Protocol.decode_fixed_string(rest, 4)
      assert v1 == <<"ab", 0, 0>>
      assert v2 == "abcd"
    end
  end

  # ---------------------------------------------------------------------------
  # Edge cases
  # ---------------------------------------------------------------------------

  describe "edge cases" do
    test "single row block" do
      columns = [{"val", "UInt64", [42]}]
      result = BlockEncoder.encode_data_block(columns, @rev) |> IO.iodata_to_binary()
      rest = skip_data_block_header(result)

      {"val", rest} = Protocol.decode_string(rest)
      {"UInt64", rest} = Protocol.decode_string(rest)
      {0, rest} = Protocol.decode_uint8(rest)
      {42, <<>>} = Protocol.decode_uint64(rest)
    end

    test "zero values" do
      encoded =
        BlockEncoder.encode_column_values("UInt64", [0, 0, 0]) |> IO.iodata_to_binary()

      assert byte_size(encoded) == 24
      {0, rest} = Protocol.decode_uint64(encoded)
      {0, rest} = Protocol.decode_uint64(rest)
      {0, <<>>} = Protocol.decode_uint64(rest)
    end

    test "max UInt64 value" do
      max = 0xFFFF_FFFF_FFFF_FFFF
      encoded = BlockEncoder.encode_column_values("UInt64", [max]) |> IO.iodata_to_binary()
      {^max, <<>>} = Protocol.decode_uint64(encoded)
    end

    test "empty string values" do
      encoded =
        BlockEncoder.encode_column_values("String", ["", "", ""]) |> IO.iodata_to_binary()

      {"", rest} = Protocol.decode_string(encoded)
      {"", rest} = Protocol.decode_string(rest)
      {"", <<>>} = Protocol.decode_string(rest)
    end

    test "Float64 with integer input" do
      encoded = BlockEncoder.encode_column_values("Float64", [42]) |> IO.iodata_to_binary()
      assert byte_size(encoded) == 8
      {42.0, <<>>} = Protocol.decode_float64(encoded)
    end
  end

  # ---------------------------------------------------------------------------
  # extract_inner_type/1
  # ---------------------------------------------------------------------------

  describe "extract_inner_type/1" do
    test "simple type" do
      assert BlockEncoder.extract_inner_type("Int64)") == "Int64"
    end

    test "nested type with parens" do
      assert BlockEncoder.extract_inner_type("DateTime64(9))") == "DateTime64(9)"
    end

    test "deeply nested type" do
      assert BlockEncoder.extract_inner_type("JSON(max_dynamic_paths=0, max_dynamic_types=1))") ==
               "JSON(max_dynamic_paths=0, max_dynamic_types=1)"
    end

    test "LowCardinality(String)" do
      assert BlockEncoder.extract_inner_type("String)") == "String"
    end
  end

  # ---------------------------------------------------------------------------
  # Nullable column
  # ---------------------------------------------------------------------------

  describe "Nullable column" do
    test "Nullable(Int64) with nils" do
      encoded =
        BlockEncoder.encode_column_values("Nullable(Int64)", [nil, 42, nil])
        |> IO.iodata_to_binary()

      # Null mask: 3 UInt8 bytes
      {1, rest} = Protocol.decode_uint8(encoded)
      {0, rest} = Protocol.decode_uint8(rest)
      {1, rest} = Protocol.decode_uint8(rest)

      # Values: 3 Int64 (null positions get default 0)
      {0, rest} = Protocol.decode_int64(rest)
      {42, rest} = Protocol.decode_int64(rest)
      {0, <<>>} = Protocol.decode_int64(rest)
    end

    test "Nullable(DateTime64(9)) with nils" do
      ts = 1_700_000_000_123_456_789

      encoded =
        BlockEncoder.encode_column_values("Nullable(DateTime64(9))", [nil, ts])
        |> IO.iodata_to_binary()

      # Null mask
      {1, rest} = Protocol.decode_uint8(encoded)
      {0, rest} = Protocol.decode_uint8(rest)

      # Values
      {0, rest} = Protocol.decode_datetime64(rest)
      {^ts, <<>>} = Protocol.decode_datetime64(rest)
    end

    test "Nullable(String) with nils" do
      encoded =
        BlockEncoder.encode_column_values("Nullable(String)", [nil, "hello", nil])
        |> IO.iodata_to_binary()

      # Null mask
      {1, rest} = Protocol.decode_uint8(encoded)
      {0, rest} = Protocol.decode_uint8(rest)
      {1, rest} = Protocol.decode_uint8(rest)

      # Values
      {"", rest} = Protocol.decode_string(rest)
      {"hello", rest} = Protocol.decode_string(rest)
      {"", <<>>} = Protocol.decode_string(rest)
    end

    test "Nullable with no nils" do
      encoded =
        BlockEncoder.encode_column_values("Nullable(Int64)", [10, 20])
        |> IO.iodata_to_binary()

      {0, rest} = Protocol.decode_uint8(encoded)
      {0, rest} = Protocol.decode_uint8(rest)
      {10, rest} = Protocol.decode_int64(rest)
      {20, <<>>} = Protocol.decode_int64(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # Enum8 column
  # ---------------------------------------------------------------------------

  describe "Enum8 column" do
    test "encodes signed Int8 values" do
      encoded =
        BlockEncoder.encode_column_values(
          "Enum8('gauge' = 1, 'sum' = 2, 'histogram' = 3, 'exponential_histogram' = 4, 'summary' = 5)",
          [1, 3, 5]
        )
        |> IO.iodata_to_binary()

      assert byte_size(encoded) == 3
      {1, rest} = Protocol.decode_int8(encoded)
      {3, rest} = Protocol.decode_int8(rest)
      {5, <<>>} = Protocol.decode_int8(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # JSON column
  # ---------------------------------------------------------------------------

  describe "JSON column" do
    test "encodes maps as JSON strings" do
      encoded =
        BlockEncoder.encode_column_values(
          "JSON(max_dynamic_paths=0, max_dynamic_types=1)",
          [%{"key" => "value"}, %{}]
        )
        |> IO.iodata_to_binary()

      {json1, rest} = Protocol.decode_string(encoded)
      {json2, <<>>} = Protocol.decode_string(rest)

      assert Jason.decode!(json1) == %{"key" => "value"}
      assert Jason.decode!(json2) == %{}
    end

    test "sanitizes non-JSON-safe values" do
      encoded =
        BlockEncoder.encode_column_values(
          "JSON(max_dynamic_paths=0, max_dynamic_types=1)",
          [%{"tuple" => {1, 2}}]
        )
        |> IO.iodata_to_binary()

      {json, <<>>} = Protocol.decode_string(encoded)
      assert Jason.decode!(json) == %{"tuple" => [1, 2]}
    end
  end

  # ---------------------------------------------------------------------------
  # Array columns
  # ---------------------------------------------------------------------------

  describe "Array column" do
    test "Array(UInt64) with mixed arrays" do
      encoded =
        BlockEncoder.encode_column_values("Array(UInt64)", [[1, 2], [3], []])
        |> IO.iodata_to_binary()

      # Offsets: [2, 3, 3]
      {2, rest} = Protocol.decode_uint64(encoded)
      {3, rest} = Protocol.decode_uint64(rest)
      {3, rest} = Protocol.decode_uint64(rest)

      # Data: [1, 2, 3]
      {1, rest} = Protocol.decode_uint64(rest)
      {2, rest} = Protocol.decode_uint64(rest)
      {3, <<>>} = Protocol.decode_uint64(rest)
    end

    test "Array(Float64) values" do
      encoded =
        BlockEncoder.encode_column_values("Array(Float64)", [[1.0], [2.0, 3.0]])
        |> IO.iodata_to_binary()

      # Offsets: [1, 3]
      {1, rest} = Protocol.decode_uint64(encoded)
      {3, rest} = Protocol.decode_uint64(rest)

      # Data: [1.0, 2.0, 3.0]
      {1.0, rest} = Protocol.decode_float64(rest)
      {2.0, rest} = Protocol.decode_float64(rest)
      {3.0, <<>>} = Protocol.decode_float64(rest)
    end

    test "Array(String) values" do
      encoded =
        BlockEncoder.encode_column_values("Array(String)", [["a", "b"], ["c"]])
        |> IO.iodata_to_binary()

      # Offsets: [2, 3]
      {2, rest} = Protocol.decode_uint64(encoded)
      {3, rest} = Protocol.decode_uint64(rest)

      # Data: ["a", "b", "c"]
      {"a", rest} = Protocol.decode_string(rest)
      {"b", rest} = Protocol.decode_string(rest)
      {"c", <<>>} = Protocol.decode_string(rest)
    end

    test "Array(DateTime64(9)) values" do
      ts1 = 1_700_000_000_000_000_000
      ts2 = 1_700_000_001_000_000_000
      ts3 = 1_700_000_002_000_000_000

      encoded =
        BlockEncoder.encode_column_values("Array(DateTime64(9))", [[ts1], [ts2, ts3]])
        |> IO.iodata_to_binary()

      # Offsets: [1, 3]
      {1, rest} = Protocol.decode_uint64(encoded)
      {3, rest} = Protocol.decode_uint64(rest)

      # Data
      {^ts1, rest} = Protocol.decode_datetime64(rest)
      {^ts2, rest} = Protocol.decode_datetime64(rest)
      {^ts3, <<>>} = Protocol.decode_datetime64(rest)
    end

    test "Array(JSON(...)) values" do
      encoded =
        BlockEncoder.encode_column_values(
          "Array(JSON(max_dynamic_paths=0, max_dynamic_types=1))",
          [[%{}, %{"a" => 1}], []]
        )
        |> IO.iodata_to_binary()

      # Offsets: [2, 2]
      {2, rest} = Protocol.decode_uint64(encoded)
      {2, rest} = Protocol.decode_uint64(rest)

      # Data: two JSON strings
      {json1, rest} = Protocol.decode_string(rest)
      {json2, <<>>} = Protocol.decode_string(rest)

      assert Jason.decode!(json1) == %{}
      assert Jason.decode!(json2) == %{"a" => 1}
    end

    test "all empty arrays" do
      encoded =
        BlockEncoder.encode_column_values("Array(UInt64)", [[], [], []])
        |> IO.iodata_to_binary()

      # Offsets: [0, 0, 0]
      {0, rest} = Protocol.decode_uint64(encoded)
      {0, rest} = Protocol.decode_uint64(rest)
      {0, <<>>} = Protocol.decode_uint64(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # LowCardinality column (treated as inner type with setting)
  # ---------------------------------------------------------------------------

  describe "LowCardinality column" do
    test "encodes as inner String type" do
      encoded =
        BlockEncoder.encode_column_values("LowCardinality(String)", ["a", "b"])
        |> IO.iodata_to_binary()

      {"a", rest} = Protocol.decode_string(encoded)
      {"b", <<>>} = Protocol.decode_string(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # Custom serialization flag behavior
  # ---------------------------------------------------------------------------

  describe "custom serialization flag" do
    test "included when rev >= 54454 and rows > 0" do
      columns = [{"x", "UInt8", [1]}]
      result = BlockEncoder.encode_data_block(columns, 54_454) |> IO.iodata_to_binary()
      rest = skip_data_block_header(result)

      {"x", rest} = Protocol.decode_string(rest)
      {"UInt8", rest} = Protocol.decode_string(rest)
      # Custom serialization flag should be present
      {0, rest} = Protocol.decode_uint8(rest)
      {1, <<>>} = Protocol.decode_uint8(rest)
    end

    test "omitted when rev < 54454" do
      columns = [{"x", "UInt8", [1]}]
      result = BlockEncoder.encode_data_block(columns, 54_453) |> IO.iodata_to_binary()
      rest = skip_data_block_header(result)

      {"x", rest} = Protocol.decode_string(rest)
      {"UInt8", rest} = Protocol.decode_string(rest)
      # No custom serialization flag — next byte is the column data
      {1, <<>>} = Protocol.decode_uint8(rest)
    end
  end

  # ---------------------------------------------------------------------------
  # Test helpers
  # ---------------------------------------------------------------------------

  defp skip_block_info(data) do
    {1, rest} = Protocol.decode_varuint(data)
    {_is_overflows, rest} = Protocol.decode_uint8(rest)
    {2, rest} = Protocol.decode_varuint(rest)
    {_bucket_num, rest} = Protocol.decode_int32(rest)
    {0, rest} = Protocol.decode_varuint(rest)
    rest
  end

  defp skip_data_block_header(data) do
    {_packet_type, rest} = Protocol.decode_varuint(data)
    {_temp_table, rest} = Protocol.decode_string(rest)
    rest = skip_block_info(rest)
    {_num_cols, rest} = Protocol.decode_varuint(rest)
    {_num_rows, rest} = Protocol.decode_varuint(rest)
    rest
  end
end
