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

    test "a corrupted payload (CRC mismatch) with nothing else to recover reports :corrupt with an empty list" do
      segment = Framing.encode_segment("hello world")
      <<len::32-big, crc::32-big, "hello world">> = segment
      corrupted = <<len::32-big, crc::32-big, "HELLO world">>

      assert {:error, :corrupt, []} = Framing.decode_segments(corrupted)
    end

    test "a corrupted segment among intact ones still returns the intact ones" do
      good = Framing.encode_segment("one")
      <<len::32-big, crc::32-big, "two">> = Framing.encode_segment("two")
      bad = <<len::32-big, crc::32-big, "TWO">>

      assert {:error, :corrupt, ["one"]} = Framing.decode_segments(good <> bad)
    end

    test "reports :not_framed for a truncated frame (declared length longer than remaining data)" do
      segment = Framing.encode_segment("hello world")
      truncated = binary_part(segment, 0, byte_size(segment) - 3)

      assert {:error, :not_framed} = Framing.decode_segments(truncated)
    end

    test "trailing garbage after a valid segment reports :corrupt with the valid segment recovered" do
      valid = Framing.encode_segment("hello")

      assert {:error, :corrupt, ["hello"]} = Framing.decode_segments(valid <> <<1, 2, 3>>)
    end
  end

  describe "recover!/1" do
    test "returns 0 for a missing file" do
      path = Path.join(System.tmp_dir!(), "missing_#{System.unique_integer([:positive])}.wal")

      assert Framing.recover!(path) == 0
    end

    test "truncates a torn tail and returns the valid byte offset" do
      path = Path.join(System.tmp_dir!(), "torn_#{System.unique_integer([:positive])}.wal")
      whole = Framing.encode_segment("whole\n")
      torn = binary_part(Framing.encode_segment("torn\n"), 0, 5)
      File.write!(path, whole <> torn)
      on_exit(fn -> File.rm(path) end)

      assert Framing.recover!(path) == byte_size(whole)
      assert {:ok, contents} = File.read(path)
      assert contents == whole
    end

    test "handles a read error other than :enoent (e.g. the path is a directory) without crashing" do
      # A directory read fails at the OS level (:eisdir) regardless of
      # permissions/user — unlike a chmod-based simulation, this is reliable
      # even when tests run as root (as some CI containers do).
      dir = Path.join(System.tmp_dir!(), "not_a_file_#{System.unique_integer([:positive])}")
      File.mkdir_p!(dir)
      on_exit(fn -> File.rm_rf!(dir) end)

      assert Framing.recover!(dir) == 0
    end
  end
end
