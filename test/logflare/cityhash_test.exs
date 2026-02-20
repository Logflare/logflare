defmodule Logflare.CityHashTest do
  use ExUnit.Case, async: true

  alias Logflare.CityHash

  describe "hash128/1" do
    test "returns a 16-byte binary" do
      result = CityHash.hash128("hello")
      assert byte_size(result) == 16
    end

    test "empty input produces a valid 16-byte hash" do
      result = CityHash.hash128("")
      assert byte_size(result) == 16
    end

    test "deterministic — same input always produces same output" do
      input = "test data for hashing"
      assert CityHash.hash128(input) == CityHash.hash128(input)
    end

    test "different inputs produce different hashes" do
      a = CityHash.hash128("hello")
      b = CityHash.hash128("world")
      assert a != b
    end

    test "works with large binary input" do
      data = :crypto.strong_rand_bytes(1_048_576)
      result = CityHash.hash128(data)
      assert byte_size(result) == 16
    end

    test "works with raw binary (non-UTF8) input" do
      data = <<0, 1, 2, 255, 254, 253, 0, 0, 128>>
      result = CityHash.hash128(data)
      assert byte_size(result) == 16
    end

    test "single byte inputs produce distinct hashes" do
      hashes = for i <- 0..255, do: CityHash.hash128(<<i>>)
      unique = Enum.uniq(hashes)
      assert length(unique) == 256
    end

    test "regression vectors" do
      assert CityHash.hash128("") == Base.decode16!("291EE592C340B53C2B9AC064FC9DF03D")
      assert CityHash.hash128("a") == Base.decode16!("F66CC8E4E28E7EFDD01AE0AFA13971D2")
      assert CityHash.hash128("test") == Base.decode16!("49F73450774CD69289F6AED8A064CFF9")
      assert CityHash.hash128("ClickHouse") == Base.decode16!("0214F9BAC43AD47612B60BC081CC0437")
    end
  end
end
