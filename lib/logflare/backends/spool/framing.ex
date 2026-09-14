defmodule Logflare.Backends.Spool.Framing do
  @moduledoc """
  Length+CRC32-prefixed framing for spool segments — lets independently
  encoded chunk payloads be concatenated into one file and split back
  apart safely.

  Frame layout: `<<byte_size(payload)::32-big, crc32(payload)::32-big, payload::binary>>`.
  """

  require Logger

  @spec encode_segment(binary()) :: binary()
  def encode_segment(payload) when is_binary(payload) do
    <<byte_size(payload)::32-big, :erlang.crc32(payload)::32-big, payload::binary>>
  end

  @doc """
  Splits a concatenated sequence of frames back into their payloads,
  verifying each frame's CRC32. Stops at the first failure (a torn write,
  or a frame whose CRC doesn't match) instead of trying to skip past it —
  once a frame's own length can't be trusted, there's no reliable way to
  know where the next one starts, so a "corrupt" frame and a torn tail are
  handled identically: keep whatever decoded cleanly before it, discard
  the rest as untrustworthy.

    * `{:ok, segments}` — every byte was consumed by valid frames.
    * `{:error, :corrupt, segments}` — stopped early; `segments` is
      whatever decoded cleanly before the failure (possibly empty).
  """
  @spec decode_segments(binary()) :: {:ok, [binary()]} | {:error, :corrupt, [binary()]}
  def decode_segments(binary) when is_binary(binary), do: decode_segments(binary, [])

  defp decode_segments(<<>>, acc), do: {:ok, Enum.reverse(acc)}

  defp decode_segments(
         <<len::32-big, crc::32-big, payload::binary-size(len), rest::binary>>,
         acc
       ) do
    if :erlang.crc32(payload) == crc do
      decode_segments(rest, [payload | acc])
    else
      Logger.warning(
        "spool_framing: corrupt frame (#{len} bytes, CRC mismatch) after " <>
          "#{length(acc)} valid segment(s) — discarding the rest as untrustworthy"
      )

      {:error, :corrupt, Enum.reverse(acc)}
    end
  end

  defp decode_segments(torn, acc) do
    Logger.warning(
      "spool_framing: torn/unparseable frame after #{length(acc)} valid segment(s) " <>
        "(#{byte_size(torn)} byte(s) left over) — salvaging what was recoverable"
    )

    {:error, :corrupt, Enum.reverse(acc)}
  end

  @doc """
  Reads a WAL file, truncating any torn tail in place (a write that never
  finished before a crash). Returns the byte offset at which the next frame
  should be appended. A missing file is treated as an empty log.
  """
  @spec recover!(Path.t()) :: non_neg_integer()
  def recover!(path) do
    case File.read(path) do
      {:ok, contents} ->
        case decode_segments(contents) do
          {:ok, segments} ->
            valid_byte_size(segments)

          {:error, :corrupt, segments} ->
            valid = valid_byte_size(segments)
            truncate!(path, valid)
            valid
        end

      {:error, :enoent} ->
        0

      {:error, reason} ->
        Logger.warning(
          "spool_framing: failed to read #{path} during recovery, starting from offset 0: #{inspect(reason)}"
        )

        0
    end
  end

  # Frames have no padding between them, so the on-disk size of a
  # recovered prefix is always derivable from the payloads alone.
  defp valid_byte_size(segments), do: Enum.reduce(segments, 0, &(byte_size(&1) + 8 + &2))

  defp truncate!(path, valid) do
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {:ok, _} = :file.position(fd, valid)
    :ok = :file.truncate(fd)
    :ok = :file.close(fd)
  end
end
