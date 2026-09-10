defmodule Logflare.Backends.Spool.EncoderTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.Backends.Spool.Framing
  alias Logflare.LogEvent

  defp log_event(body, via_rule_id \\ nil) do
    %LogEvent{
      id: Ecto.UUID.generate(),
      source_id: 1,
      body: body,
      event_type: :log,
      ingested_at: DateTime.utc_now(),
      valid: true,
      drop: false,
      via_rule_id: via_rule_id
    }
  end

  describe "encode_chunk/2" do
    test "ndjson round-trips to one JSON line per event, framed, uncompressed" do
      events = [log_event(%{"message" => "one"}), log_event(%{"message" => "two"})]

      {segment, raw_byte_size} = Encoder.encode_chunk(events, :ndjson)

      assert {:ok, [body]} = Framing.decode_segments(segment)
      assert byte_size(body) == raw_byte_size

      lines = body |> String.trim() |> String.split("\n") |> Enum.map(&Jason.decode!/1)
      assert [%{"body" => %{"message" => "one"}}, %{"body" => %{"message" => "two"}}] = lines
    end

    test "etf round-trips to a single term of records, framed, uncompressed" do
      events = [log_event(%{"message" => "hello"}, 123)]

      {segment, raw_byte_size} = Encoder.encode_chunk(events, :etf)

      assert {:ok, [body]} = Framing.decode_segments(segment)
      assert byte_size(body) == raw_byte_size
      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(body)
    end
  end

  describe "compress_binary/2" do
    test "gzip compresses and round-trips" do
      compressed = Encoder.compress_binary(:gzip, "hello world")
      assert :zlib.gunzip(compressed) == "hello world"
    end

    test "zstd compresses and round-trips" do
      compressed = Encoder.compress_binary(:zstd, "hello world")
      assert :ezstd.decompress(compressed) == "hello world"
    end
  end

  describe "file_extension/3" do
    test "reflects format/compress/algorithm" do
      assert Encoder.file_extension(:ndjson, false, :gzip) == "ndjson"
      assert Encoder.file_extension(:etf, false, :zstd) == "etf"
      assert Encoder.file_extension(:ndjson, true, :gzip) == "ndjson.gz"
      assert Encoder.file_extension(:ndjson, true, :zstd) == "ndjson.zst"
      assert Encoder.file_extension(:etf, true, :gzip) == "etf.gz"
      assert Encoder.file_extension(:etf, true, :zstd) == "etf.zst"
    end
  end

  describe "format_tag/3" do
    test "reflects format/compress/algorithm" do
      assert Encoder.format_tag(:ndjson, false, :gzip) == :ndjson
      assert Encoder.format_tag(:etf, false, :zstd) == :etf
      assert Encoder.format_tag(:ndjson, true, :gzip) == :ndjson_gz
      assert Encoder.format_tag(:ndjson, true, :zstd) == :ndjson_zstd
      assert Encoder.format_tag(:etf, true, :gzip) == :etf_gz
      assert Encoder.format_tag(:etf, true, :zstd) == :etf_zstd
    end
  end

  describe "content_encoding/2" do
    test "nil when not compressing, algorithm name otherwise" do
      assert Encoder.content_encoding(false, :gzip) == nil
      assert Encoder.content_encoding(true, :gzip) == "gzip"
      assert Encoder.content_encoding(true, :zstd) == "zstd"
    end
  end

  describe "file_key_with_version/2 and file_key_version/1" do
    test "tags a key with the current version, and it round-trips back out" do
      key = Encoder.file_key_with_version("0/some-uuid", "ndjson")

      assert key == "0/some-uuid.v#{Encoder.current_version()}.ndjson"
      assert Encoder.file_key_version(key) == Encoder.current_version()
    end

    test "a key with no version tag at all is :legacy" do
      assert Encoder.file_key_version("0/some-uuid.ndjson") == :legacy
      assert Encoder.file_key_version("0/some-uuid.ndjson.gz") == :legacy
    end

    test "a key tagged with some other version number extracts that number, not :legacy" do
      assert Encoder.file_key_version("0/some-uuid.v3.ndjson") == 3
      assert Encoder.file_key_version("0/some-uuid.v1.ndjson") == 1
    end
  end
end
