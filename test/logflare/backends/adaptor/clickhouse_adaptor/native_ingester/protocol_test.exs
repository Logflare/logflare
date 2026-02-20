defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ProtocolTest do
  @moduledoc false

  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Protocol

  # ---------------------------------------------------------------------------
  # VarUInt encode/decode round-trip
  # ---------------------------------------------------------------------------

  describe "encode_varuint/1 and decode_varuint/1" do
    test "encodes and decodes 0" do
      assert Protocol.encode_varuint(0) == <<0>>
      assert Protocol.decode_varuint(<<0>>) == {0, <<>>}
    end

    test "encodes and decodes single-byte values (0-127)" do
      for value <- [0, 1, 42, 127] do
        encoded = Protocol.encode_varuint(value)
        assert byte_size(encoded) == 1
        assert Protocol.decode_varuint(encoded) == {value, <<>>}
      end
    end

    test "encodes and decodes two-byte values (128-16383)" do
      for value <- [128, 300, 16_383] do
        encoded = Protocol.encode_varuint(value)
        assert byte_size(encoded) == 2
        assert Protocol.decode_varuint(encoded) == {value, <<>>}
      end
    end

    test "encodes 300 as 0xAC 0x02" do
      assert Protocol.encode_varuint(300) == <<0xAC, 0x02>>
    end

    test "encodes and decodes multi-byte values" do
      for value <- [16_384, 65_535, 1_000_000, 54_483] do
        encoded = Protocol.encode_varuint(value)
        assert Protocol.decode_varuint(encoded) == {value, <<>>}
      end
    end

    test "encodes and decodes large values" do
      # Max UInt64-ish values
      for value <- [0xFFFF_FFFF, 0xFFFF_FFFF_FFFF_FFFF] do
        encoded = Protocol.encode_varuint(value)
        assert Protocol.decode_varuint(encoded) == {value, <<>>}
      end
    end

    test "preserves trailing bytes after decode" do
      encoded = Protocol.encode_varuint(42) <> <<0xFF, 0xAB>>
      assert Protocol.decode_varuint(encoded) == {42, <<0xFF, 0xAB>>}
    end
  end

  # ---------------------------------------------------------------------------
  # String encode/decode round-trip
  # ---------------------------------------------------------------------------

  describe "encode_string/1 and decode_string/1" do
    test "encodes and decodes empty string" do
      encoded = Protocol.encode_string("")
      assert encoded == <<0>>
      assert Protocol.decode_string(encoded) == {"", <<>>}
    end

    test "encodes and decodes short ASCII string" do
      encoded = Protocol.encode_string("hello")
      assert encoded == <<5, "hello">>
      assert Protocol.decode_string(encoded) == {"hello", <<>>}
    end

    test "encodes and decodes UTF-8 string" do
      str = "héllo wörld"
      encoded = Protocol.encode_string(str)
      assert Protocol.decode_string(encoded) == {str, <<>>}
    end

    test "encodes and decodes long string" do
      str = String.duplicate("x", 300)
      encoded = Protocol.encode_string(str)
      assert Protocol.decode_string(encoded) == {str, <<>>}
    end

    test "preserves trailing bytes after decode" do
      encoded = Protocol.encode_string("abc") <> <<0xFF>>
      assert Protocol.decode_string(encoded) == {"abc", <<0xFF>>}
    end
  end

  # ---------------------------------------------------------------------------
  # Boolean encode/decode
  # ---------------------------------------------------------------------------

  describe "encode_bool/1 and decode_bool/1" do
    test "encodes true as 0x01" do
      assert Protocol.encode_bool(true) == <<1>>
    end

    test "encodes false as 0x00" do
      assert Protocol.encode_bool(false) == <<0>>
    end

    test "round-trips" do
      assert Protocol.decode_bool(Protocol.encode_bool(true)) == {true, <<>>}
      assert Protocol.decode_bool(Protocol.encode_bool(false)) == {false, <<>>}
    end
  end

  # ---------------------------------------------------------------------------
  # UInt8 / Int8
  # ---------------------------------------------------------------------------

  describe "uint8 encode/decode" do
    test "round-trips" do
      for value <- [0, 1, 127, 255] do
        assert {^value, <<>>} = Protocol.decode_uint8(Protocol.encode_uint8(value))
      end
    end
  end

  describe "int8 encode/decode" do
    test "round-trips" do
      for value <- [-128, -1, 0, 1, 127] do
        assert {^value, <<>>} = Protocol.decode_int8(Protocol.encode_int8(value))
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Enum8 (signed Int8 alias)
  # ---------------------------------------------------------------------------

  describe "enum8 encode" do
    test "encodes as signed Int8" do
      for value <- [-128, -1, 0, 1, 127] do
        assert Protocol.encode_enum8(value) == Protocol.encode_int8(value)
      end
    end
  end

  # ---------------------------------------------------------------------------
  # UInt16 / Int16
  # ---------------------------------------------------------------------------

  describe "uint16 encode/decode" do
    test "round-trips" do
      for value <- [0, 1, 256, 65_535] do
        assert {^value, <<>>} = Protocol.decode_uint16(Protocol.encode_uint16(value))
      end
    end

    test "uses little-endian" do
      assert Protocol.encode_uint16(0x0102) == <<0x02, 0x01>>
    end
  end

  describe "int16 encode/decode" do
    test "round-trips" do
      for value <- [-32_768, -1, 0, 1, 32_767] do
        assert {^value, <<>>} = Protocol.decode_int16(Protocol.encode_int16(value))
      end
    end
  end

  # ---------------------------------------------------------------------------
  # UInt32 / Int32
  # ---------------------------------------------------------------------------

  describe "uint32 encode/decode" do
    test "round-trips" do
      for value <- [0, 1, 65_536, 0xFFFF_FFFF] do
        assert {^value, <<>>} = Protocol.decode_uint32(Protocol.encode_uint32(value))
      end
    end

    test "uses little-endian" do
      assert Protocol.encode_uint32(0x01020304) == <<0x04, 0x03, 0x02, 0x01>>
    end
  end

  describe "int32 encode/decode" do
    test "round-trips" do
      for value <- [-2_147_483_648, -1, 0, 1, 2_147_483_647] do
        assert {^value, <<>>} = Protocol.decode_int32(Protocol.encode_int32(value))
      end
    end
  end

  # ---------------------------------------------------------------------------
  # UInt64 / Int64
  # ---------------------------------------------------------------------------

  describe "uint64 encode/decode" do
    test "round-trips" do
      for value <- [0, 1, 0xFFFF_FFFF_FFFF_FFFF] do
        assert {^value, <<>>} = Protocol.decode_uint64(Protocol.encode_uint64(value))
      end
    end
  end

  describe "int64 encode/decode" do
    test "round-trips" do
      for value <- [-9_223_372_036_854_775_808, -1, 0, 1, 9_223_372_036_854_775_807] do
        assert {^value, <<>>} = Protocol.decode_int64(Protocol.encode_int64(value))
      end
    end
  end

  # ---------------------------------------------------------------------------
  # UInt128 / Int128
  # ---------------------------------------------------------------------------

  describe "uint128 encode/decode" do
    test "round-trips" do
      for value <- [0, 1, 0xFFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF_FFFF] do
        assert {^value, <<>>} = Protocol.decode_uint128(Protocol.encode_uint128(value))
      end
    end
  end

  describe "int128 encode/decode" do
    test "round-trips" do
      max = 170_141_183_460_469_231_731_687_303_715_884_105_727
      min = -170_141_183_460_469_231_731_687_303_715_884_105_728

      for value <- [min, -1, 0, 1, max] do
        assert {^value, <<>>} = Protocol.decode_int128(Protocol.encode_int128(value))
      end
    end
  end

  # ---------------------------------------------------------------------------
  # Float32 / Float64
  # ---------------------------------------------------------------------------

  describe "float32 encode/decode" do
    test "round-trips" do
      for value <- [0.0, 1.0, -1.0, 3.14] do
        {decoded, <<>>} = Protocol.decode_float32(Protocol.encode_float32(value))
        assert_in_delta decoded, value, 0.001
      end
    end

    test "encodes to 4 bytes" do
      assert byte_size(Protocol.encode_float32(1.0)) == 4
    end
  end

  describe "float64 encode/decode" do
    test "round-trips" do
      for value <- [0.0, 1.0, -1.0, 3.141592653589793, 1.0e308] do
        assert {^value, <<>>} = Protocol.decode_float64(Protocol.encode_float64(value))
      end
    end

    test "encodes to 8 bytes" do
      assert byte_size(Protocol.encode_float64(1.0)) == 8
    end

    test "accepts integer input" do
      {decoded, <<>>} = Protocol.decode_float64(Protocol.encode_float64(42))
      assert decoded == 42.0
    end
  end

  # ---------------------------------------------------------------------------
  # UUID encode/decode
  # ---------------------------------------------------------------------------

  describe "encode_uuid/1 and decode_uuid/1" do
    test "round-trips raw 16-byte UUID" do
      raw = <<1::64, 2::64>>
      encoded = Protocol.encode_uuid(raw)
      assert byte_size(encoded) == 16
      assert Protocol.decode_uuid(encoded) == {raw, <<>>}
    end

    test "encodes UUID string" do
      uuid_str = "550e8400-e29b-41d4-a716-446655440000"
      encoded = Protocol.encode_uuid(uuid_str)
      assert byte_size(encoded) == 16
      {decoded_raw, <<>>} = Protocol.decode_uuid(encoded)
      assert byte_size(decoded_raw) == 16
    end

    test "uses little-endian for each 8-byte half" do
      raw =
        <<0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0A, 0x0B, 0x0C, 0x0D, 0x0E,
          0x0F, 0x10>>

      encoded = Protocol.encode_uuid(raw)
      <<u1::little-unsigned-64, u2::little-unsigned-64>> = encoded
      <<o1::unsigned-64, o2::unsigned-64>> = raw
      assert u1 == o1
      assert u2 == o2
    end
  end

  # ---------------------------------------------------------------------------
  # DateTime / DateTime64
  # ---------------------------------------------------------------------------

  describe "encode_datetime/1 and decode_datetime/1" do
    test "round-trips" do
      for value <- [0, 1_700_000_000, 0xFFFF_FFFF] do
        assert {^value, <<>>} = Protocol.decode_datetime(Protocol.encode_datetime(value))
      end
    end

    test "encodes to 4 bytes" do
      assert byte_size(Protocol.encode_datetime(0)) == 4
    end
  end

  describe "encode_datetime64/2 and decode_datetime64/1" do
    test "round-trips" do
      for value <- [0, 1_700_000_000_000_000_000, -1_000_000_000] do
        assert {^value, <<>>} = Protocol.decode_datetime64(Protocol.encode_datetime64(value, 9))
      end
    end

    test "encodes to 8 bytes" do
      assert byte_size(Protocol.encode_datetime64(0, 9)) == 8
    end
  end

  # ---------------------------------------------------------------------------
  # FixedString
  # ---------------------------------------------------------------------------

  describe "encode_fixed_string/2 and decode_fixed_string/2" do
    test "pads short strings with null bytes" do
      assert Protocol.encode_fixed_string("ab", 4) == <<"ab", 0, 0>>
    end

    test "passes through exact-length strings" do
      assert Protocol.encode_fixed_string("abcd", 4) == "abcd"
    end

    test "truncates long strings" do
      assert Protocol.encode_fixed_string("abcde", 4) == "abcd"
    end

    test "round-trips" do
      encoded = Protocol.encode_fixed_string("abc", 5)
      assert Protocol.decode_fixed_string(encoded, 5) == {<<"abc", 0, 0>>, <<>>}
    end
  end

  # ---------------------------------------------------------------------------
  # Packet type constants
  # ---------------------------------------------------------------------------

  describe "packet type constants" do
    test "client packet types" do
      assert Protocol.client_hello() == 0
      assert Protocol.client_query() == 1
      assert Protocol.client_data() == 2
      assert Protocol.client_cancel() == 3
      assert Protocol.client_ping() == 4
    end

    test "server packet types" do
      assert Protocol.server_hello() == 0
      assert Protocol.server_data() == 1
      assert Protocol.server_exception() == 2
      assert Protocol.server_progress() == 3
      assert Protocol.server_pong() == 4
      assert Protocol.server_end_of_stream() == 5
      assert Protocol.server_profile_info() == 6
      assert Protocol.server_totals() == 7
      assert Protocol.server_extremes() == 8
      assert Protocol.server_tables_status_response() == 9
      assert Protocol.server_log() == 10
      assert Protocol.server_table_columns() == 11
      assert Protocol.server_profile_events() == 14
    end
  end

  # ---------------------------------------------------------------------------
  # Protocol version constants
  # ---------------------------------------------------------------------------

  describe "protocol version constants" do
    test "current version" do
      assert Protocol.dbms_tcp_protocol_version() == 54_483
    end

    test "feature gate revisions are in ascending order" do
      revisions = [
        Protocol.min_revision_with_block_info(),
        Protocol.min_revision_with_client_info(),
        Protocol.min_revision_with_server_timezone(),
        Protocol.min_revision_with_server_display_name(),
        Protocol.min_revision_with_version_patch(),
        Protocol.min_revision_with_low_cardinality_type(),
        Protocol.min_revision_with_settings_serialized_as_strings(),
        Protocol.min_revision_with_interserver_secret(),
        Protocol.min_revision_with_opentelemetry(),
        Protocol.min_revision_with_distributed_depth(),
        Protocol.min_revision_with_initial_query_start_time(),
        Protocol.min_revision_with_custom_serialization(),
        Protocol.min_protocol_version_with_addendum(),
        Protocol.min_protocol_version_with_parameters(),
        Protocol.min_protocol_version_with_password_complexity_rules(),
        Protocol.min_revision_with_interserver_secret_v2(),
        Protocol.min_revision_with_server_query_time_in_progress(),
        Protocol.min_protocol_version_with_chunked_packets(),
        Protocol.min_revision_with_versioned_parallel_replicas_protocol(),
        Protocol.min_revision_with_server_settings(),
        Protocol.min_revision_with_query_plan_serialization(),
        Protocol.min_revision_with_versioned_cluster_function_protocol()
      ]

      sorted = Enum.sort(revisions)
      assert revisions == sorted
    end
  end

  # ---------------------------------------------------------------------------
  # Client identity constants
  # ---------------------------------------------------------------------------

  describe "client identity" do
    test "client name is set" do
      assert Protocol.client_name() == "logflare-native"
    end

    test "version numbers are positive integers" do
      assert Protocol.client_version_major() > 0
      assert Protocol.client_version_minor() >= 0
      assert Protocol.client_version_patch() >= 0
    end
  end
end
