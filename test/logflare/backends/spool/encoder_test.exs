defmodule Logflare.Backends.Spool.EncoderTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Spool.Encoder
  alias Logflare.LogEvent

  defp log_event(body, via_rule_id) do
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

  describe "encode_raw_chunk/1" do
    test "round-trips to a single term of records, with an event-count header" do
      events = [log_event(%{"message" => "hello"}, 123)]

      raw = Encoder.encode_raw_chunk(events)

      assert <<1::32-big, etf::binary>> = raw
      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(etf)
    end
  end

  describe "decode_segments/1 and count_events/1" do
    test "decode_segments strips each segment's event-count header" do
      segment = Encoder.encode_raw_chunk([log_event(%{"message" => "hello"}, 123)])
      {frame, _size} = DurableBuffer.WAL.encode(segment)
      file_body = IO.iodata_to_binary(frame)

      assert {[etf], _valid, _rest} = Encoder.decode_segments(file_body)
      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(etf)
    end

    test "count_events sums each segment's header without decoding etf" do
      segment_1 = Encoder.encode_raw_chunk([log_event(%{"message" => "a"}, 1)])

      segment_2 =
        Encoder.encode_raw_chunk([
          log_event(%{"message" => "b"}, 2),
          log_event(%{"message" => "c"}, 3)
        ])

      file_body =
        [segment_1, segment_2]
        |> Enum.map(fn segment ->
          {frame, _size} = DurableBuffer.WAL.encode(segment)
          frame
        end)
        |> IO.iodata_to_binary()

      assert Encoder.count_events(file_body) == 3
    end
  end

  describe "compress_binary/1" do
    test "zstd compresses and round-trips" do
      compressed = Encoder.compress_binary("hello world")
      assert :ezstd.decompress(compressed) == "hello world"
    end
  end

  describe "file_extension/1" do
    test "reflects compress" do
      assert Encoder.file_extension(false) == "etf"
      assert Encoder.file_extension(true) == "etf.zst"
    end
  end

  describe "content_encoding/1" do
    test "nil when not compressing, zstd otherwise" do
      assert Encoder.content_encoding(false) == nil
      assert Encoder.content_encoding(true) == "zstd"
    end
  end

  describe "file_key_with_version/2 and file_key_version/1" do
    test "tags a key with the current version, and it round-trips back out" do
      key = Encoder.file_key_with_version("0/some-uuid", "etf")

      assert key == "0/some-uuid.v#{Encoder.current_version()}.etf"
      assert Encoder.file_key_version(key) == Encoder.current_version()
    end

    test "a key with no version tag at all is :legacy" do
      assert Encoder.file_key_version("0/some-uuid.etf") == :legacy
      assert Encoder.file_key_version("0/some-uuid.etf.zst") == :legacy
    end

    test "a key tagged with some other version number extracts that number, not :legacy" do
      assert Encoder.file_key_version("0/some-uuid.v3.etf") == 3
      assert Encoder.file_key_version("0/some-uuid.v1.etf") == 1
    end
  end
end
