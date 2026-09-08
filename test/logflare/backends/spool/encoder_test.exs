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

  describe "encode_chunk/4" do
    test "ndjson uncompressed round-trips to one JSON line per event, framed" do
      events = [log_event(%{"message" => "one"}), log_event(%{"message" => "two"})]

      {segment, compressed_byte_size, raw_byte_size, format_tag} =
        Encoder.encode_chunk(events, :ndjson, false, :gzip)

      assert format_tag == :ndjson
      assert {:ok, [body]} = Framing.decode_segments(segment)
      assert byte_size(body) == compressed_byte_size
      assert compressed_byte_size == raw_byte_size

      lines = body |> String.trim() |> String.split("\n") |> Enum.map(&Jason.decode!/1)
      assert [%{"body" => %{"message" => "one"}}, %{"body" => %{"message" => "two"}}] = lines
    end

    for {algorithm, decompress} <- [
          {:gzip, &:zlib.gunzip/1},
          {:zstd, &:ezstd.decompress/1}
        ] do
      test "ndjson+#{algorithm} round-trips through decompression" do
        events = [log_event(%{"message" => "hello"}, 123)]

        {segment, _compressed_byte_size, raw_byte_size, format_tag} =
          Encoder.encode_chunk(events, :ndjson, true, unquote(algorithm))

        assert raw_byte_size > 0
        assert format_tag == unquote(:"ndjson_#{if algorithm == :gzip, do: "gz", else: "zstd"}")
        assert {:ok, [body]} = Framing.decode_segments(segment)

        line = body |> unquote(decompress).() |> String.trim() |> Jason.decode!()
        assert %{"via_rule_id" => 123} = line
      end

      test "etf+#{algorithm} round-trips through decompression and binary_to_term" do
        events = [log_event(%{"message" => "hello"}, 123)]

        {segment, _compressed_byte_size, raw_byte_size, format_tag} =
          Encoder.encode_chunk(events, :etf, true, unquote(algorithm))

        assert raw_byte_size > 0
        assert format_tag == unquote(:"etf_#{if algorithm == :gzip, do: "gz", else: "zstd"}")
        assert {:ok, [body]} = Framing.decode_segments(segment)

        assert [%{via_rule_id: 123}] = body |> unquote(decompress).() |> :erlang.binary_to_term()
      end
    end

    test "etf uncompressed round-trips to a single term of records, framed" do
      events = [log_event(%{"message" => "hello"}, 123)]

      {segment, compressed_byte_size, raw_byte_size, format_tag} =
        Encoder.encode_chunk(events, :etf, false, :gzip)

      assert compressed_byte_size == raw_byte_size
      assert format_tag == :etf
      assert {:ok, [body]} = Framing.decode_segments(segment)
      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(body)
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
