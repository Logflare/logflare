defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoderTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.RowBinaryEncoder

  describe "varint/1" do
    test "encodes zero" do
      assert RowBinaryEncoder.varint(0) == <<0>>
    end

    test "encodes values less than 128" do
      assert RowBinaryEncoder.varint(1) == <<1>>
      assert RowBinaryEncoder.varint(127) == <<127>>
    end

    test "encodes 128 (requires 2 bytes)" do
      assert RowBinaryEncoder.varint(128) == <<0x80, 0x01>>
    end

    test "encodes 300" do
      assert RowBinaryEncoder.varint(300) == <<0xAC, 0x02>>
    end

    test "encodes larger numbers" do
      assert RowBinaryEncoder.varint(16_384) == <<0x80, 0x80, 0x01>>
    end
  end

  describe "uuid/1" do
    test "encodes UUID string to 16 bytes" do
      uuid = "550e8400-e29b-41d4-a716-446655440000"
      encoded = RowBinaryEncoder.uuid(uuid)

      assert byte_size(encoded) == 16

      assert encoded ==
               <<0xD4, 0x41, 0x9B, 0xE2, 0x00, 0x84, 0x0E, 0x55, 0x00, 0x00, 0x44, 0x55, 0x66,
                 0x44, 0x16, 0xA7>>
    end

    test "raises an exception for invalid UUIDs" do
      assert_raise ArgumentError,
                   "invalid UUID: \"6E6F6F626172\"",
                   fn ->
                     RowBinaryEncoder.uuid("6E6F6F626172")
                   end
    end

    test "handles uppercase UUIDs" do
      uuid = "550E8400-E29B-41D4-A716-446655440000"
      encoded = RowBinaryEncoder.uuid(uuid)

      assert byte_size(encoded) == 16
    end

    test "handles UUIDs without dashes" do
      uuid = "550e8400e29b41d4a716446655440000"
      encoded = RowBinaryEncoder.uuid(uuid)

      assert byte_size(encoded) == 16
    end
  end

  describe "string/1" do
    test "encodes binary string with varint length prefix" do
      encoded = RowBinaryEncoder.string("hello")

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<5, "hello">>
    end

    test "encodes simple iodata with varint length prefix" do
      encoded = RowBinaryEncoder.string(["hello"])

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<5, "hello">>
    end

    test "encodes longer iodata" do
      long_string = String.duplicate("a", 200)
      encoded = RowBinaryEncoder.string([long_string])

      assert is_list(encoded)
      binary = IO.iodata_to_binary(encoded)
      <<length_bytes::binary-size(2), content::binary>> = binary
      assert length_bytes == <<0xC8, 0x01>>
      assert content == long_string
    end

    test "encodes iodata containing UTF-8 characters correctly" do
      encoded = RowBinaryEncoder.string(["café"])

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<5, "café">>
    end

    test "encodes iodata containing emoji without issues" do
      encoded = RowBinaryEncoder.string(["🚀"])

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<4, "🚀">>
    end

    test "encodes complex iodata without intermediate binary allocation" do
      iodata = ["hello", " ", "world"]
      encoded = RowBinaryEncoder.string(iodata)

      assert is_list(encoded)
      assert IO.iodata_length(encoded) == 1 + 11
      assert IO.iodata_to_binary(encoded) == <<11, "hello world">>
    end

    test "encodes empty string" do
      encoded = RowBinaryEncoder.string("")

      assert is_list(encoded)
      assert IO.iodata_to_binary(encoded) == <<0>>
    end
  end

  describe "fixed_string/2" do
    test "encodes string of exact length" do
      encoded = RowBinaryEncoder.fixed_string("hello", 5)
      assert encoded == "hello"
      assert byte_size(encoded) == 5
    end

    test "pads shorter strings with null bytes" do
      encoded = RowBinaryEncoder.fixed_string("hi", 5)
      assert encoded == <<"hi", 0, 0, 0>>
      assert byte_size(encoded) == 5
    end

    test "truncates longer strings" do
      encoded = RowBinaryEncoder.fixed_string("hello world", 5)
      assert encoded == "hello"
      assert byte_size(encoded) == 5
    end

    test "handles empty string" do
      encoded = RowBinaryEncoder.fixed_string("", 3)
      assert encoded == <<0, 0, 0>>
      assert byte_size(encoded) == 3
    end

    test "handles single character" do
      encoded = RowBinaryEncoder.fixed_string("x", 1)
      assert encoded == "x"
      assert byte_size(encoded) == 1
    end
  end

  describe "bool/1" do
    test "encodes true as 0x01" do
      assert RowBinaryEncoder.bool(true) == <<1>>
    end

    test "encodes false as 0x00" do
      assert RowBinaryEncoder.bool(false) == <<0>>
    end
  end

  describe "uint8/1" do
    test "encodes 0" do
      assert RowBinaryEncoder.uint8(0) == <<0>>
    end

    test "encodes 255" do
      assert RowBinaryEncoder.uint8(255) == <<255>>
    end

    test "encodes mid-range value" do
      assert RowBinaryEncoder.uint8(128) == <<128>>
    end
  end

  describe "uint16/1" do
    test "encodes 0" do
      assert RowBinaryEncoder.uint16(0) == <<0, 0>>
    end

    test "encodes max value" do
      assert RowBinaryEncoder.uint16(65_535) == <<255, 255>>
    end

    test "uses little-endian byte order" do
      assert RowBinaryEncoder.uint16(256) == <<0, 1>>
      assert RowBinaryEncoder.uint16(1) == <<1, 0>>
    end
  end

  describe "uint32/1" do
    test "encodes 0" do
      assert RowBinaryEncoder.uint32(0) == <<0, 0, 0, 0>>
    end

    test "encodes max value" do
      assert RowBinaryEncoder.uint32(4_294_967_295) == <<255, 255, 255, 255>>
    end

    test "uses little-endian byte order" do
      assert RowBinaryEncoder.uint32(256) == <<0, 1, 0, 0>>
      assert RowBinaryEncoder.uint32(65_536) == <<0, 0, 1, 0>>
    end
  end

  describe "uint64/1" do
    test "encodes 0" do
      assert RowBinaryEncoder.uint64(0) == <<0, 0, 0, 0, 0, 0, 0, 0>>
    end

    test "encodes large values" do
      assert RowBinaryEncoder.uint64(0xFFFFFFFFFFFFFFFF) ==
               <<255, 255, 255, 255, 255, 255, 255, 255>>
    end

    test "uses little-endian byte order" do
      assert RowBinaryEncoder.uint64(256) == <<0, 1, 0, 0, 0, 0, 0, 0>>
    end
  end

  describe "uint128/1" do
    test "encodes 0" do
      encoded = RowBinaryEncoder.uint128(0)
      assert byte_size(encoded) == 16
      assert encoded == <<0::128>>
    end

    test "encodes large values" do
      # Max UInt128: 2^128 - 1
      max_uint128 = Integer.pow(2, 128) - 1
      encoded = RowBinaryEncoder.uint128(max_uint128)
      assert byte_size(encoded) == 16
      assert encoded == :binary.copy(<<255>>, 16)
    end

    test "uses little-endian byte order" do
      encoded = RowBinaryEncoder.uint128(256)
      <<first_two::binary-size(2), rest::binary>> = encoded
      assert first_two == <<0, 1>>
      assert rest == <<0::112>>
    end
  end

  describe "uint256/1" do
    test "encodes 0" do
      encoded = RowBinaryEncoder.uint256(0)
      assert byte_size(encoded) == 32
      assert encoded == <<0::256>>
    end

    test "encodes large values" do
      # Max UInt256: 2^256 - 1
      max_uint256 = Integer.pow(2, 256) - 1
      encoded = RowBinaryEncoder.uint256(max_uint256)
      assert byte_size(encoded) == 32
      assert encoded == :binary.copy(<<255>>, 32)
    end
  end

  describe "int8/1" do
    test "encodes positive values" do
      assert RowBinaryEncoder.int8(1) == <<1>>
      assert RowBinaryEncoder.int8(127) == <<127>>
    end

    test "encodes negative values" do
      assert RowBinaryEncoder.int8(-1) == <<255>>
      assert RowBinaryEncoder.int8(-128) == <<128>>
    end

    test "encodes zero" do
      assert RowBinaryEncoder.int8(0) == <<0>>
    end
  end

  describe "int16/1" do
    test "encodes positive values" do
      assert RowBinaryEncoder.int16(1) == <<1, 0>>
      assert RowBinaryEncoder.int16(256) == <<0, 1>>
    end

    test "encodes negative values" do
      assert RowBinaryEncoder.int16(-1) == <<255, 255>>
      assert RowBinaryEncoder.int16(-256) == <<0, 255>>
    end

    test "encodes zero" do
      assert RowBinaryEncoder.int16(0) == <<0, 0>>
    end
  end

  describe "int32/1" do
    test "encodes positive values" do
      assert RowBinaryEncoder.int32(1) == <<1, 0, 0, 0>>
      assert RowBinaryEncoder.int32(256) == <<0, 1, 0, 0>>
    end

    test "encodes negative values" do
      assert RowBinaryEncoder.int32(-1) == <<255, 255, 255, 255>>
      assert RowBinaryEncoder.int32(-256) == <<0, 255, 255, 255>>
    end

    test "encodes zero" do
      assert RowBinaryEncoder.int32(0) == <<0, 0, 0, 0>>
    end
  end

  describe "int64/1" do
    test "encodes positive values" do
      assert RowBinaryEncoder.int64(1) == <<1, 0, 0, 0, 0, 0, 0, 0>>
      assert RowBinaryEncoder.int64(256) == <<0, 1, 0, 0, 0, 0, 0, 0>>
    end

    test "encodes negative values" do
      assert RowBinaryEncoder.int64(-1) == <<255, 255, 255, 255, 255, 255, 255, 255>>
    end

    test "encodes zero" do
      assert RowBinaryEncoder.int64(0) == <<0, 0, 0, 0, 0, 0, 0, 0>>
    end
  end

  describe "int128/1" do
    test "encodes 0" do
      encoded = RowBinaryEncoder.int128(0)
      assert byte_size(encoded) == 16
      assert encoded == <<0::128>>
    end

    test "encodes positive values" do
      encoded = RowBinaryEncoder.int128(1)
      assert byte_size(encoded) == 16
      <<first, rest::binary>> = encoded
      assert first == 1
      assert rest == <<0::120>>
    end

    test "encodes negative values" do
      encoded = RowBinaryEncoder.int128(-1)
      assert byte_size(encoded) == 16
      assert encoded == :binary.copy(<<255>>, 16)
    end
  end

  describe "int256/1" do
    test "encodes 0" do
      encoded = RowBinaryEncoder.int256(0)
      assert byte_size(encoded) == 32
      assert encoded == <<0::256>>
    end

    test "encodes positive values" do
      encoded = RowBinaryEncoder.int256(1)
      assert byte_size(encoded) == 32
      <<first, rest::binary>> = encoded
      assert first == 1
      assert rest == <<0::248>>
    end

    test "encodes negative values" do
      encoded = RowBinaryEncoder.int256(-1)
      assert byte_size(encoded) == 32
      assert encoded == :binary.copy(<<255>>, 32)
    end
  end

  describe "float32/1" do
    test "encodes positive floats" do
      encoded = RowBinaryEncoder.float32(1.0)
      assert byte_size(encoded) == 4
      <<decoded::little-float-32>> = encoded
      assert decoded == 1.0
    end

    test "encodes negative floats" do
      encoded = RowBinaryEncoder.float32(-1.0)
      <<decoded::little-float-32>> = encoded
      assert decoded == -1.0
    end

    test "encodes zero" do
      encoded = RowBinaryEncoder.float32(0.0)
      <<decoded::little-float-32>> = encoded
      assert decoded == 0.0
    end

    test "encodes integers as floats" do
      encoded = RowBinaryEncoder.float32(42)
      <<decoded::little-float-32>> = encoded
      assert decoded == 42.0
    end
  end

  describe "float64/1" do
    test "encodes positive floats" do
      encoded = RowBinaryEncoder.float64(1.0)
      assert byte_size(encoded) == 8
      <<decoded::little-float-64>> = encoded
      assert decoded == 1.0
    end

    test "encodes negative floats" do
      encoded = RowBinaryEncoder.float64(-1.0)
      <<decoded::little-float-64>> = encoded
      assert decoded == -1.0
    end

    test "encodes zero" do
      encoded = RowBinaryEncoder.float64(0.0)
      <<decoded::little-float-64>> = encoded
      assert decoded == 0.0
    end

    test "encodes integers as floats" do
      encoded = RowBinaryEncoder.float64(42)
      <<decoded::little-float-64>> = encoded
      assert decoded == 42.0
    end

    test "maintains precision" do
      value = 3.141592653589793
      encoded = RowBinaryEncoder.float64(value)
      <<decoded::little-float-64>> = encoded
      assert decoded == value
    end
  end

  describe "enum8/1" do
    test "encodes positive values" do
      assert RowBinaryEncoder.enum8(1) == <<1>>
      assert RowBinaryEncoder.enum8(127) == <<127>>
    end

    test "encodes negative values" do
      assert RowBinaryEncoder.enum8(-1) == <<255>>
      assert RowBinaryEncoder.enum8(-128) == <<128>>
    end

    test "encodes zero" do
      assert RowBinaryEncoder.enum8(0) == <<0>>
    end
  end

  describe "enum16/1" do
    test "encodes positive values" do
      assert RowBinaryEncoder.enum16(1) == <<1, 0>>
      assert RowBinaryEncoder.enum16(256) == <<0, 1>>
      assert RowBinaryEncoder.enum16(32_767) == <<255, 127>>
    end

    test "encodes negative values" do
      assert RowBinaryEncoder.enum16(-1) == <<255, 255>>
      assert RowBinaryEncoder.enum16(-32_768) == <<0, 128>>
    end

    test "encodes zero" do
      assert RowBinaryEncoder.enum16(0) == <<0, 0>>
    end
  end

  describe "ipv4/1" do
    test "encodes tuple format" do
      encoded = RowBinaryEncoder.ipv4({192, 168, 1, 1})
      assert encoded == <<192, 168, 1, 1>>
      assert byte_size(encoded) == 4
    end

    test "encodes string format" do
      encoded = RowBinaryEncoder.ipv4("10.0.0.1")
      assert encoded == <<10, 0, 0, 1>>
    end

    test "encodes localhost" do
      assert RowBinaryEncoder.ipv4({127, 0, 0, 1}) == <<127, 0, 0, 1>>
      assert RowBinaryEncoder.ipv4("127.0.0.1") == <<127, 0, 0, 1>>
    end

    test "encodes broadcast address" do
      assert RowBinaryEncoder.ipv4({255, 255, 255, 255}) == <<255, 255, 255, 255>>
    end
  end

  describe "ipv6/1" do
    test "encodes tuple format" do
      encoded = RowBinaryEncoder.ipv6({0x2001, 0x0DB8, 0, 0, 0, 0, 0, 1})
      assert byte_size(encoded) == 16
      assert encoded == <<0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>
    end

    test "encodes string format" do
      encoded = RowBinaryEncoder.ipv6("::1")
      assert byte_size(encoded) == 16
      assert encoded == <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>
    end

    test "encodes full address string" do
      encoded = RowBinaryEncoder.ipv6("2001:db8::1")
      assert byte_size(encoded) == 16
      assert encoded == <<0x20, 0x01, 0x0D, 0xB8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1>>
    end
  end

  describe "date/1" do
    test "encodes epoch as zero" do
      epoch = ~D[1970-01-01]
      encoded = RowBinaryEncoder.date(epoch)

      assert byte_size(encoded) == 2
      <<days::little-unsigned-16>> = encoded
      assert days == 0
    end

    test "encodes date as days since epoch" do
      date = ~D[2024-01-01]
      encoded = RowBinaryEncoder.date(date)

      assert byte_size(encoded) == 2
      <<days::little-unsigned-16>> = encoded
      expected_days = Date.diff(date, ~D[1970-01-01])
      assert days == expected_days
    end

    test "uses little-endian byte order" do
      date = ~D[1970-02-05]
      encoded = RowBinaryEncoder.date(date)

      <<days::little-unsigned-16>> = encoded
      assert days == 35
    end
  end

  describe "date32/1" do
    test "encodes epoch as zero" do
      epoch = ~D[1970-01-01]
      encoded = RowBinaryEncoder.date32(epoch)

      assert byte_size(encoded) == 4
      <<days::little-signed-32>> = encoded
      assert days == 0
    end

    test "encodes date as days since epoch" do
      date = ~D[2024-01-01]
      encoded = RowBinaryEncoder.date32(date)

      assert byte_size(encoded) == 4
      <<days::little-signed-32>> = encoded
      expected_days = Date.diff(date, ~D[1970-01-01])
      assert days == expected_days
    end

    test "encodes dates before epoch as negative" do
      date = ~D[1969-12-31]
      encoded = RowBinaryEncoder.date32(date)

      <<days::little-signed-32>> = encoded
      assert days == -1
    end
  end

  describe "time/1" do
    test "encodes midnight as zero" do
      midnight = ~T[00:00:00]
      encoded = RowBinaryEncoder.time(midnight)

      assert byte_size(encoded) == 4
      <<seconds::little-signed-32>> = encoded
      assert seconds == 0
    end

    test "encodes time as seconds since midnight" do
      time = ~T[12:30:45]
      encoded = RowBinaryEncoder.time(time)

      <<seconds::little-signed-32>> = encoded
      expected = 12 * 3600 + 30 * 60 + 45
      assert seconds == expected
    end

    test "encodes end of day" do
      time = ~T[23:59:59]
      encoded = RowBinaryEncoder.time(time)

      <<seconds::little-signed-32>> = encoded
      expected = 23 * 3600 + 59 * 60 + 59
      assert seconds == expected
    end
  end

  describe "datetime/1" do
    test "encodes epoch as zero" do
      epoch = ~U[1970-01-01 00:00:00Z]
      encoded = RowBinaryEncoder.datetime(epoch)

      assert byte_size(encoded) == 4
      <<timestamp::little-unsigned-32>> = encoded
      assert timestamp == 0
    end

    test "encodes current timestamp" do
      datetime = ~U[2024-01-01 12:30:45Z]
      encoded = RowBinaryEncoder.datetime(datetime)

      <<timestamp::little-unsigned-32>> = encoded
      assert timestamp == DateTime.to_unix(datetime, :second)
    end

    test "uses 4-byte little-endian unsigned" do
      datetime = ~U[2024-01-01 00:00:00Z]
      encoded = RowBinaryEncoder.datetime(datetime)

      assert byte_size(encoded) == 4
    end
  end

  describe "datetime64/2" do
    test "encodes with microsecond precision (6) by default" do
      datetime = ~U[2024-01-01 12:30:45.123456Z]
      encoded = RowBinaryEncoder.datetime64(datetime)

      assert byte_size(encoded) == 8

      <<timestamp_int::little-signed-64>> = encoded
      expected = DateTime.to_unix(datetime, :microsecond)
      assert timestamp_int == expected
    end

    test "encodes with nanosecond precision (9)" do
      datetime = ~U[2024-01-01 12:30:45.123456Z]
      encoded = RowBinaryEncoder.datetime64(datetime, 9)

      <<timestamp_int::little-signed-64>> = encoded
      expected_seconds = DateTime.to_unix(datetime, :second)
      expected = expected_seconds * 1_000_000_000 + 123_456_000
      assert timestamp_int == expected
    end

    test "encodes with second precision (0)" do
      datetime = ~U[2024-01-01 12:30:45.123456Z]
      encoded = RowBinaryEncoder.datetime64(datetime, 0)

      <<timestamp_int::little-signed-64>> = encoded
      expected = DateTime.to_unix(datetime, :second)
      assert timestamp_int == expected
    end

    test "encodes with millisecond precision (3)" do
      datetime = ~U[2024-01-01 12:30:45.123456Z]
      encoded = RowBinaryEncoder.datetime64(datetime, 3)

      <<timestamp_int::little-signed-64>> = encoded
      expected_seconds = DateTime.to_unix(datetime, :second)
      expected = expected_seconds * 1_000 + 123
      assert timestamp_int == expected
    end

    test "encodes epoch correctly" do
      epoch = ~U[1970-01-01 00:00:00.000000Z]
      encoded = RowBinaryEncoder.datetime64(epoch, 6)

      <<timestamp_int::little-signed-64>> = encoded
      assert timestamp_int == 0
    end

    test "handles microsecond precision" do
      datetime = ~U[2024-01-01 00:00:00.123456Z]
      encoded = RowBinaryEncoder.datetime64(datetime, 6)

      <<timestamp_int::little-signed-64>> = encoded
      expected_seconds = DateTime.to_unix(datetime, :second)
      expected = expected_seconds * 1_000_000 + 123_456
      assert timestamp_int == expected
    end
  end

  describe "datetime64_from_unix/3" do
    test "encodes microseconds to nanosecond precision (9)" do
      timestamp_us = DateTime.to_unix(~U[2024-01-01 12:30:45.123456Z], :microsecond)
      encoded = RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9)

      assert byte_size(encoded) == 8
      <<scaled::little-signed-64>> = encoded
      assert scaled == timestamp_us * 1_000
    end

    test "encodes microseconds to microsecond precision (6) as identity" do
      timestamp_us = DateTime.to_unix(~U[2024-01-01 12:30:45.123456Z], :microsecond)
      encoded = RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 6)

      <<scaled::little-signed-64>> = encoded
      assert scaled == timestamp_us
    end

    test "encodes microseconds to millisecond precision (3)" do
      timestamp_us = DateTime.to_unix(~U[2024-01-01 12:30:45.123456Z], :microsecond)
      encoded = RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 3)

      <<scaled::little-signed-64>> = encoded
      assert scaled == div(timestamp_us, 1_000)
    end

    test "encodes microseconds to second precision (0)" do
      timestamp_us = DateTime.to_unix(~U[2024-01-01 12:30:45.123456Z], :microsecond)
      encoded = RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 0)

      <<scaled::little-signed-64>> = encoded
      assert scaled == div(timestamp_us, 1_000_000)
    end

    test "encodes seconds to nanosecond precision (9)" do
      timestamp_s = DateTime.to_unix(~U[2024-01-01 12:30:45Z], :second)
      encoded = RowBinaryEncoder.datetime64_from_unix(timestamp_s, :second, 9)

      <<scaled::little-signed-64>> = encoded
      assert scaled == timestamp_s * 1_000_000_000
    end

    test "encodes milliseconds to microsecond precision (6)" do
      timestamp_ms = DateTime.to_unix(~U[2024-01-01 12:30:45.123Z], :millisecond)
      encoded = RowBinaryEncoder.datetime64_from_unix(timestamp_ms, :millisecond, 6)

      <<scaled::little-signed-64>> = encoded
      assert scaled == timestamp_ms * 1_000
    end

    test "produces same result as datetime64/2 for equivalent input" do
      datetime = ~U[2024-01-01 12:30:45.123456Z]
      timestamp_us = DateTime.to_unix(datetime, :microsecond)

      from_struct = RowBinaryEncoder.datetime64(datetime, 9)
      from_unix = RowBinaryEncoder.datetime64_from_unix(timestamp_us, :microsecond, 9)

      assert from_struct == from_unix
    end

    test "encodes epoch as zero" do
      encoded = RowBinaryEncoder.datetime64_from_unix(0, :microsecond, 9)

      <<scaled::little-signed-64>> = encoded
      assert scaled == 0
    end
  end

  describe "json/1" do
    test "encodes map as JSON string" do
      value = %{"key" => "value"}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<length, json_string::binary>> = binary
      assert length == 15
      assert Jason.decode!(json_string) == value
    end

    test "encodes list as JSON string" do
      value = [1, 2, 3]
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<length, json_string::binary>> = binary
      assert length == 7
      assert Jason.decode!(json_string) == value
    end

    test "encodes nested structures" do
      value = %{"nested" => %{"array" => [1, 2, 3]}}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      assert Jason.decode!(json_string) == value
    end

    test "sanitizes port values to strings" do
      port = Port.open({:spawn, "cat"}, [:binary])
      value = %{"port" => port}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      decoded = Jason.decode!(json_string)
      assert is_binary(decoded["port"])
      assert String.starts_with?(decoded["port"], "#Port<")
      Port.close(port)
    end

    test "sanitizes pid values to strings" do
      value = %{"pid" => self()}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      decoded = Jason.decode!(json_string)
      assert is_binary(decoded["pid"])
      assert String.starts_with?(decoded["pid"], "#PID<")
    end

    test "sanitizes reference values to strings" do
      ref = make_ref()
      value = %{"ref" => ref}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      decoded = Jason.decode!(json_string)
      assert is_binary(decoded["ref"])
      assert String.starts_with?(decoded["ref"], "#Reference<")
    end

    test "sanitizes function values to strings" do
      value = %{"fun" => &String.length/1}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      decoded = Jason.decode!(json_string)
      assert is_binary(decoded["fun"])
      assert String.starts_with?(decoded["fun"], "&")
    end

    test "sanitizes nested non-serializable values" do
      port = Port.open({:spawn, "cat"}, [:binary])

      value = %{
        "outer" => %{
          "inner" => [1, port, %{"deep" => self()}]
        }
      }

      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      decoded = Jason.decode!(json_string)
      inner = decoded["outer"]["inner"]
      assert Enum.at(inner, 0) == 1
      assert is_binary(Enum.at(inner, 1))
      assert is_binary(Enum.at(inner, 2)["deep"])
      Port.close(port)
    end

    test "converts tuples to lists" do
      value = %{"tuple" => {1, "two", 3}}
      encoded = RowBinaryEncoder.json(value)

      binary = IO.iodata_to_binary(encoded)
      <<_length, json_string::binary>> = binary
      decoded = Jason.decode!(json_string)
      assert decoded["tuple"] == [1, "two", 3]
    end
  end

  describe "nullable/2" do
    test "encodes nil as single byte 1" do
      encoded = RowBinaryEncoder.nullable(nil, &RowBinaryEncoder.uint32/1)
      assert IO.iodata_to_binary(encoded) == <<1>>
    end

    test "encodes non-nil value with 0 prefix" do
      encoded = RowBinaryEncoder.nullable(42, &RowBinaryEncoder.uint32/1)
      binary = IO.iodata_to_binary(encoded)
      assert binary == <<0, 42, 0, 0, 0>>
    end

    test "works with string encoder" do
      encoded = RowBinaryEncoder.nullable("hello", &RowBinaryEncoder.string/1)
      binary = IO.iodata_to_binary(encoded)
      assert binary == <<0, 5, "hello">>
    end

    test "works with nil and string encoder" do
      encoded = RowBinaryEncoder.nullable(nil, &RowBinaryEncoder.string/1)
      assert IO.iodata_to_binary(encoded) == <<1>>
    end

    test "works with complex types" do
      encoded = RowBinaryEncoder.nullable(%{"a" => 1}, &RowBinaryEncoder.json/1)
      binary = IO.iodata_to_binary(encoded)
      <<flag, rest::binary>> = binary
      assert flag == 0
      assert byte_size(rest) > 0
    end
  end

  describe "array/2" do
    test "encodes empty array" do
      encoded = RowBinaryEncoder.array([], &RowBinaryEncoder.uint8/1)

      assert IO.iodata_to_binary(encoded) == <<0>>
    end

    test "encodes array with custom encoder" do
      encoded = RowBinaryEncoder.array([1, 2, 3], &RowBinaryEncoder.uint8/1)

      binary = IO.iodata_to_binary(encoded)
      assert binary == <<3, 1, 2, 3>>
    end

    test "varint-prefixes the count" do
      items = Enum.to_list(1..200)
      encoded = RowBinaryEncoder.array(items, &RowBinaryEncoder.uint8/1)

      binary = IO.iodata_to_binary(encoded)
      <<0xC8, 0x01, rest::binary>> = binary
      assert byte_size(rest) == 200
    end
  end

  describe "array_string/1" do
    test "encodes string array" do
      encoded = RowBinaryEncoder.array_string(["hello", "world"])

      binary = IO.iodata_to_binary(encoded)
      assert binary == <<2, 5, "hello", 5, "world">>
    end

    test "handles empty strings in array" do
      encoded = RowBinaryEncoder.array_string(["", "test", ""])

      binary = IO.iodata_to_binary(encoded)
      assert binary == <<3, 0, 4, "test", 0>>
    end

    test "handles empty array" do
      encoded = RowBinaryEncoder.array_string([])

      assert IO.iodata_to_binary(encoded) == <<0>>
    end
  end

  describe "array_uint64/1" do
    test "encodes uint64 array" do
      encoded = RowBinaryEncoder.array_uint64([1, 2, 3])

      binary = IO.iodata_to_binary(encoded)

      assert binary ==
               <<3, 1, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0>>
    end

    test "handles empty array" do
      encoded = RowBinaryEncoder.array_uint64([])

      assert IO.iodata_to_binary(encoded) == <<0>>
    end
  end

  describe "array_float64/1" do
    test "encodes float64 array" do
      encoded = RowBinaryEncoder.array_float64([1.0, 2.0, 3.0])

      binary = IO.iodata_to_binary(encoded)
      <<count, f1::little-float-64, f2::little-float-64, f3::little-float-64>> = binary
      assert count == 3
      assert f1 == 1.0
      assert f2 == 2.0
      assert f3 == 3.0
    end

    test "handles empty array" do
      encoded = RowBinaryEncoder.array_float64([])

      assert IO.iodata_to_binary(encoded) == <<0>>
    end
  end

  describe "array_json/1" do
    test "encodes array of JSON objects" do
      encoded = RowBinaryEncoder.array_json([%{"a" => 1}, %{"b" => 2}])

      binary = IO.iodata_to_binary(encoded)
      <<count, rest::binary>> = binary
      assert count == 2
      assert byte_size(rest) > 0
    end

    test "handles empty array" do
      encoded = RowBinaryEncoder.array_json([])

      assert IO.iodata_to_binary(encoded) == <<0>>
    end
  end

  describe "array_datetime64/2" do
    test "encodes array of datetime64 values" do
      dt1 = ~U[2024-01-01 00:00:00.000000Z]
      dt2 = ~U[2024-01-02 00:00:00.000000Z]
      encoded = RowBinaryEncoder.array_datetime64([dt1, dt2])

      binary = IO.iodata_to_binary(encoded)
      <<count, ts1::little-signed-64, ts2::little-signed-64>> = binary
      assert count == 2
      assert ts1 == DateTime.to_unix(dt1, :microsecond)
      assert ts2 == DateTime.to_unix(dt2, :microsecond)
    end

    test "respects precision parameter" do
      dt = ~U[2024-01-01 00:00:00.123456Z]
      encoded = RowBinaryEncoder.array_datetime64([dt], 3)

      binary = IO.iodata_to_binary(encoded)
      <<count, ts::little-signed-64>> = binary
      assert count == 1
      expected = DateTime.to_unix(dt, :second) * 1_000 + 123
      assert ts == expected
    end

    test "handles empty array" do
      encoded = RowBinaryEncoder.array_datetime64([])

      assert IO.iodata_to_binary(encoded) == <<0>>
    end
  end

  describe "map/3" do
    test "encodes empty map" do
      encoded = RowBinaryEncoder.map(%{}, &RowBinaryEncoder.string/1, &RowBinaryEncoder.string/1)

      assert IO.iodata_to_binary(encoded) == <<0>>
    end

    test "encodes empty list of pairs" do
      encoded = RowBinaryEncoder.map([], &RowBinaryEncoder.string/1, &RowBinaryEncoder.string/1)

      assert IO.iodata_to_binary(encoded) == <<0>>
    end

    test "encodes map with custom encoders" do
      encoded =
        RowBinaryEncoder.map(
          %{"a" => 1, "b" => 2},
          &RowBinaryEncoder.string/1,
          &RowBinaryEncoder.uint8/1
        )

      binary = IO.iodata_to_binary(encoded)
      # count(2) + keys(string "a" + string "b") + values(uint8 1 + uint8 2)
      # Note: Map order is not guaranteed, so we check structure
      <<count, rest::binary>> = binary
      assert count == 2
      # 2 strings (1 byte len + 1 byte char each) + 2 uint8 values = 6 bytes
      assert byte_size(rest) == 6
    end

    test "encodes list of pairs" do
      encoded =
        RowBinaryEncoder.map(
          [{"x", 10}, {"y", 20}],
          &RowBinaryEncoder.string/1,
          &RowBinaryEncoder.uint8/1
        )

      binary = IO.iodata_to_binary(encoded)
      # Preserves order for list input, interleaved key-value pairs
      # count(2) + (string("x") + uint8(10)) + (string("y") + uint8(20))
      assert binary == <<2, 1, "x", 10, 1, "y", 20>>
    end

    test "encodes as interleaved key-value pairs" do
      encoded =
        RowBinaryEncoder.map(
          [{"key1", "val1"}, {"key2", "val2"}],
          &RowBinaryEncoder.string/1,
          &RowBinaryEncoder.string/1
        )

      binary = IO.iodata_to_binary(encoded)
      # count + key1 + val1 + key2 + val2
      assert binary == <<2, 4, "key1", 4, "val1", 4, "key2", 4, "val2">>
    end
  end

  describe "map_string_string/1" do
    test "encodes Map(String, String) as interleaved key-value pairs" do
      encoded = RowBinaryEncoder.map_string_string([{"name", "alice"}, {"city", "boston"}])

      binary = IO.iodata_to_binary(encoded)
      # RowBinary Map = Array(Tuple(K,V)): count(2) + key1 + val1 + key2 + val2
      assert binary == <<2, 4, "name", 5, "alice", 4, "city", 6, "boston">>
    end

    test "handles empty map" do
      encoded = RowBinaryEncoder.map_string_string(%{})

      assert IO.iodata_to_binary(encoded) == <<0>>
    end

    test "handles map input" do
      encoded = RowBinaryEncoder.map_string_string(%{"a" => "1"})

      binary = IO.iodata_to_binary(encoded)
      <<count, rest::binary>> = binary
      assert count == 1
      # string("a") = 2 bytes, string("1") = 2 bytes
      assert byte_size(rest) == 4
    end
  end

  describe "map_string_uint64/1" do
    test "encodes Map(String, UInt64)" do
      encoded = RowBinaryEncoder.map_string_uint64([{"count", 100}, {"total", 500}])

      binary = IO.iodata_to_binary(encoded)
      # count(2) + (string("count") + uint64(100)) + (string("total") + uint64(500))
      <<count, rest::binary>> = binary
      assert count == 2
      # 2 × (string + uint64) = (6 + 8) + (6 + 8) = 28 bytes
      assert byte_size(rest) == 28
    end

    test "handles empty map" do
      assert IO.iodata_to_binary(RowBinaryEncoder.map_string_uint64(%{})) == <<0>>
    end
  end

  describe "map_string_int32/1" do
    test "encodes Map(String, Int32)" do
      encoded = RowBinaryEncoder.map_string_int32([{"x", -10}, {"y", 20}])

      binary = IO.iodata_to_binary(encoded)
      <<count, rest::binary>> = binary
      assert count == 2
      # 2 × (string + int32) = (2 + 4) + (2 + 4) = 12 bytes
      assert byte_size(rest) == 12
    end

    test "handles negative values" do
      encoded = RowBinaryEncoder.map_string_int32([{"neg", -1}])

      binary = IO.iodata_to_binary(encoded)
      # count(1) + string("neg") + int32(-1)
      assert binary == <<1, 3, "neg", 255, 255, 255, 255>>
    end
  end

  describe "map_string_float64/1" do
    test "encodes Map(String, Float64)" do
      encoded = RowBinaryEncoder.map_string_float64([{"pi", 3.14159}])

      binary = IO.iodata_to_binary(encoded)
      <<count, key_len, key::binary-size(2), value::little-float-64>> = binary
      assert count == 1
      assert key_len == 2
      assert key == "pi"
      assert_in_delta value, 3.14159, 0.00001
    end

    test "handles empty map" do
      assert IO.iodata_to_binary(RowBinaryEncoder.map_string_float64(%{})) == <<0>>
    end
  end

  describe "map_string_bool/1" do
    test "encodes Map(String, Bool) as interleaved key-value pairs" do
      encoded = RowBinaryEncoder.map_string_bool([{"enabled", true}, {"visible", false}])

      binary = IO.iodata_to_binary(encoded)
      # RowBinary Map = Array(Tuple(K,V)): count(2) + key1 + val1 + key2 + val2
      assert binary == <<2, 7, "enabled", 1, 7, "visible", 0>>
    end

    test "handles empty map" do
      assert IO.iodata_to_binary(RowBinaryEncoder.map_string_bool(%{})) == <<0>>
    end
  end

  describe "map_string_json/1" do
    test "encodes Map(String, JSON)" do
      encoded = RowBinaryEncoder.map_string_json([{"data", %{"nested" => true}}])

      binary = IO.iodata_to_binary(encoded)
      <<count, rest::binary>> = binary
      assert count == 1
      assert byte_size(rest) > 0
    end

    test "handles empty map" do
      assert IO.iodata_to_binary(RowBinaryEncoder.map_string_json(%{})) == <<0>>
    end
  end
end
