defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.CompressionTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ChCompression
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Compression

  @lz4_method 0x82
  @header_size 9

  describe "compress/1" do
    test "produces a valid compressed envelope" do
      data = "Hello, ClickHouse compression!"
      result = Compression.compress(data)

      # Envelope: 16-byte checksum + 1-byte method + 4-byte compressed size + 4-byte uncompressed size + compressed data
      assert byte_size(result) > 16 + @header_size

      <<_checksum::binary-size(16), method::8, compressed_size::little-unsigned-32,
        uncompressed_size::little-unsigned-32, _compressed_data::binary>> = result

      assert method == @lz4_method
      assert uncompressed_size == byte_size(data)
      assert compressed_size == byte_size(result) - 16 - byte_size(data) + uncompressed_size
    end

    test "checksum covers method byte through compressed data" do
      data = "test data for checksum verification"
      result = Compression.compress(data)

      <<checksum::binary-size(16), header_and_data::binary>> = result
      expected = ChCompression.hash128(header_and_data)

      assert checksum == expected
    end

    test "accepts iodata input" do
      iodata = ["Hello", [", "], "World!"]
      result = Compression.compress(iodata)

      assert is_binary(result)
      assert byte_size(result) > 16 + @header_size
    end

    test "handles empty input" do
      result = Compression.compress("")

      <<_checksum::binary-size(16), _method::8, _compressed_size::little-unsigned-32,
        uncompressed_size::little-unsigned-32, _rest::binary>> = result

      assert uncompressed_size == 0
    end

    test "compressed size includes 9-byte header" do
      data = String.duplicate("ClickHouse", 100)
      result = Compression.compress(data)

      <<_checksum::binary-size(16), _method::8, compressed_size::little-unsigned-32,
        _uncompressed_size::little-unsigned-32, compressed_data::binary>> = result

      assert compressed_size == @header_size + byte_size(compressed_data)
    end
  end

  describe "decompress/1" do
    test "round-trip: compress then decompress" do
      original = "Hello, ClickHouse native protocol compression!"
      compressed = Compression.compress(original)
      assert {:ok, ^original} = Compression.decompress(compressed)
    end

    test "round-trip with large data" do
      original = :crypto.strong_rand_bytes(100_000)
      compressed = Compression.compress(original)
      assert {:ok, ^original} = Compression.decompress(compressed)
    end

    test "round-trip with empty data" do
      compressed = Compression.compress("")
      assert {:ok, ""} = Compression.decompress(compressed)
    end

    test "round-trip with repetitive data" do
      original = String.duplicate("abcdefghijklmnop", 10_000)
      compressed = Compression.compress(original)
      assert {:ok, ^original} = Compression.decompress(compressed)
      # Verify compression actually reduced size
      assert byte_size(compressed) < byte_size(original)
    end

    test "detects checksum mismatch" do
      compressed = Compression.compress("test data")
      # Corrupt the checksum (first 16 bytes)
      <<_checksum::binary-size(16), rest::binary>> = compressed
      corrupted = <<0::128, rest::binary>>

      assert {:error, :checksum_mismatch} = Compression.decompress(corrupted)
    end

    test "rejects unsupported compression method" do
      compressed = Compression.compress("test data")
      # Replace method byte (byte 17) with an unknown method
      <<checksum::binary-size(16), _method::8, rest::binary>> = compressed
      corrupted = <<checksum::binary, 0xFF::8, rest::binary>>

      assert {:error, {:unsupported_compression_method, 0xFF}} =
               Compression.decompress(corrupted)
    end

    test "rejects truncated data" do
      assert {:error, :invalid_compressed_block} = Compression.decompress(<<0::64>>)
    end
  end
end
