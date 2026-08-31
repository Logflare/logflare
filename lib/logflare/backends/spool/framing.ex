defmodule Logflare.Backends.Spool.Framing do
  @moduledoc """
  Length+CRC32-prefixed framing for spool segments.

  A committed spool file is one or more independently-encoded (and
  optionally independently-compressed) chunk payloads concatenated
  together. Concatenation alone isn't safe to decode: `:zlib.gunzip/1`
  happens to walk concatenated gzip members transparently, but
  `:ezstd.decompress/1` does not (it silently returns only the first
  member on `decompress_streaming/2`, or errors outright on `decompress/1`)
  — and plain `:erlang.term_to_binary/1` output can't be concatenated and
  read back as one term at all. Framing removes the dependency on any of
  that: every payload (compressed or not, any format) is wrapped the same
  way, and decoding never needs to know how many chunks went into a file.

  Frame layout: `<<byte_size(payload)::32-big, crc32(payload)::32-big, payload::binary>>`.
  """

  @spec encode_segment(binary()) :: binary()
  def encode_segment(payload) when is_binary(payload) do
    <<byte_size(payload)::32-big, :erlang.crc32(payload)::32-big, payload::binary>>
  end

  @doc """
  Splits a concatenated sequence of frames back into their payloads,
  verifying each frame's CRC32. Returns `{:error, :corrupt_frame}` on the
  first checksum mismatch or truncated/malformed frame, rather than
  returning partial results — a caller can't tell a genuinely-empty file
  from a silently-truncated one otherwise.
  """
  @spec decode_segments(binary()) :: {:ok, [binary()]} | {:error, :corrupt_frame}
  def decode_segments(binary) when is_binary(binary) do
    decode_segments(binary, [])
  end

  defp decode_segments(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_segments(<<len::32-big, crc::32-big, rest::binary>>, acc)
       when byte_size(rest) >= len do
    <<payload::binary-size(len), remaining::binary>> = rest

    if :erlang.crc32(payload) == crc do
      decode_segments(remaining, [payload | acc])
    else
      {:error, :corrupt_frame}
    end
  end

  defp decode_segments(_truncated, _acc), do: {:error, :corrupt_frame}
end
