defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ChCompressionTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ChCompression

  describe "hash128/1" do
    test "returns a 16-byte binary" do
      result = ChCompression.hash128("hello")
      assert byte_size(result) == 16
    end

    test "empty input produces a valid 16-byte hash" do
      result = ChCompression.hash128("")
      assert byte_size(result) == 16
    end

    test "deterministic — same input always produces same output" do
      input = "test data for hashing"
      assert ChCompression.hash128(input) == ChCompression.hash128(input)
    end

    test "different inputs produce different hashes" do
      a = ChCompression.hash128("hello")
      b = ChCompression.hash128("world")
      assert a != b
    end

    test "works with large binary input" do
      data = :crypto.strong_rand_bytes(1_048_576)
      result = ChCompression.hash128(data)
      assert byte_size(result) == 16
    end

    test "works with raw binary (non-UTF8) input" do
      data = <<0, 1, 2, 255, 254, 253, 0, 0, 128>>
      result = ChCompression.hash128(data)
      assert byte_size(result) == 16
    end

    test "single byte inputs produce distinct hashes" do
      hashes = for i <- 0..255, do: ChCompression.hash128(<<i>>)
      unique = Enum.uniq(hashes)
      assert length(unique) == 256
    end

    test "regression vectors" do
      assert ChCompression.hash128("") == Base.decode16!("2B9AC064FC9DF03D291EE592C340B53C")
      assert ChCompression.hash128("a") == Base.decode16!("D01AE0AFA13971D2F66CC8E4E28E7EFD")
      assert ChCompression.hash128("test") == Base.decode16!("89F6AED8A064CFF949F73450774CD692")

      assert ChCompression.hash128("ClickHouse") ==
               Base.decode16!("12B60BC081CC04370214F9BAC43AD476")
    end
  end

  describe "lz4_compress/1 and lz4_decompress/2" do
    test "round-trip: compress then decompress returns original data" do
      original = "Hello, ClickHouse native protocol compression!"
      {:ok, compressed} = ChCompression.lz4_compress(original)
      {:ok, decompressed} = ChCompression.lz4_decompress(compressed, byte_size(original))
      assert decompressed == original
    end

    test "compressed output is different from input" do
      original = String.duplicate("abcdefgh", 100)
      {:ok, compressed} = ChCompression.lz4_compress(original)
      assert compressed != original
    end

    test "compressed size is smaller for repetitive data" do
      original = String.duplicate("ClickHouse", 1000)
      {:ok, compressed} = ChCompression.lz4_compress(original)
      assert byte_size(compressed) < byte_size(original)
    end

    test "round-trip with empty input" do
      {:ok, compressed} = ChCompression.lz4_compress("")
      {:ok, decompressed} = ChCompression.lz4_decompress(compressed, 0)
      assert decompressed == ""
    end

    test "round-trip with large binary data" do
      original = :crypto.strong_rand_bytes(1_048_576)
      {:ok, compressed} = ChCompression.lz4_compress(original)
      {:ok, decompressed} = ChCompression.lz4_decompress(compressed, byte_size(original))
      assert decompressed == original
    end

    test "decompress with undersized buffer returns error" do
      original = String.duplicate("ClickHouse compression test data", 100)
      {:ok, compressed} = ChCompression.lz4_compress(original)
      # Too-small buffer: LZ4 can't fit the decompressed output
      {:error, _reason} = ChCompression.lz4_decompress(compressed, 1)
    end
  end
end
