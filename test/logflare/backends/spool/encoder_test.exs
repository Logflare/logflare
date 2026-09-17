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
    test "round-trips to a single term of records, unframed" do
      events = [log_event(%{"message" => "hello"}, 123)]

      raw = Encoder.encode_raw_chunk(events)

      assert [%{via_rule_id: 123}] = :erlang.binary_to_term(raw)
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
