defmodule Logflare.Backends.Spool.FramingTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Spool.Framing

  describe "encode_segment/1 and decode_segments/1" do
    test "round-trips a single segment" do
      segment = Framing.encode_segment("hello world")

      assert {:ok, ["hello world"]} = Framing.decode_segments(segment)
    end

    test "round-trips multiple concatenated segments in order" do
      segments =
        ["one", "two", "three"]
        |> Enum.map(&Framing.encode_segment/1)
        |> IO.iodata_to_binary()

      assert {:ok, ["one", "two", "three"]} = Framing.decode_segments(segments)
    end

    test "round-trips an empty payload" do
      segment = Framing.encode_segment("")

      assert {:ok, [""]} = Framing.decode_segments(segment)
    end

    test "round-trips no segments at all" do
      assert {:ok, []} = Framing.decode_segments(<<>>)
    end

    test "round-trips binary payloads containing arbitrary bytes, not just text" do
      payload = :crypto.strong_rand_bytes(1024)
      segment = Framing.encode_segment(payload)

      assert {:ok, [^payload]} = Framing.decode_segments(segment)
    end

    test "detects a corrupted payload (CRC mismatch)" do
      segment = Framing.encode_segment("hello world")
      # Flip a bit inside the payload without touching the length/crc header.
      <<len::32-big, crc::32-big, "hello world">> = segment
      corrupted = <<len::32-big, crc::32-big, "HELLO world">>

      assert {:error, :corrupt_frame} = Framing.decode_segments(corrupted)
    end

    test "reports :not_framed for a truncated frame (declared length longer than remaining data)" do
      segment = Framing.encode_segment("hello world")
      truncated = binary_part(segment, 0, byte_size(segment) - 3)

      assert {:error, :not_framed} = Framing.decode_segments(truncated)
    end

    test "reports :not_framed for trailing garbage too short to be a valid frame header" do
      valid = Framing.encode_segment("hello")

      assert {:error, :not_framed} = Framing.decode_segments(valid <> <<1, 2, 3>>)
    end
  end
end
