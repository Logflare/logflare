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

  require Logger

  @spec encode_segment(binary()) :: binary()
  def encode_segment(payload) when is_binary(payload) do
    <<byte_size(payload)::32-big, :erlang.crc32(payload)::32-big, payload::binary>>
  end

  @doc """
  Splits a concatenated sequence of frames back into their payloads,
  verifying each frame's CRC32. Decodes as many valid segments as
  possible; a corrupt or unparseable segment is skipped (logged) rather
  than aborting the rest.

    * `{:ok, segments}` — every segment decoded cleanly.
    * `{:error, :corrupt, decoded}` — at least one segment was corrupt or
      unparseable; `decoded` has whatever was recovered (possibly empty).
    * `{:error, :not_framed}` — nothing recoverable at all.
  """
  @spec decode_segments(binary()) ::
          {:ok, [binary()]}
          | {:error, :corrupt, decoded :: [binary()]}
          | {:error, :not_framed}
  def decode_segments(binary) when is_binary(binary) do
    decode_segments(binary, [], false)
  end

  defp decode_segments(<<>>, acc, false), do: {:ok, Enum.reverse(acc)}
  defp decode_segments(<<>>, acc, true), do: {:error, :corrupt, Enum.reverse(acc)}

  defp decode_segments(<<len::32-big, crc::32-big, rest::binary>>, acc, corrupt?)
       when byte_size(rest) >= len do
    <<payload::binary-size(len), remaining::binary>> = rest

    if :erlang.crc32(payload) == crc do
      decode_segments(remaining, [payload | acc], corrupt?)
    else
      Logger.warning(
        "spool_framing: skipping corrupt segment (#{len} bytes, CRC mismatch), " <>
          "#{length(acc)} valid segment(s) so far"
      )

      decode_segments(remaining, acc, true)
    end
  end

  defp decode_segments(_torn, [], false), do: {:error, :not_framed}

  defp decode_segments(torn, acc, _corrupt?) do
    Logger.warning(
      "spool_framing: torn/unparseable frame after #{length(acc)} valid segment(s) " <>
        "(#{byte_size(torn)} byte(s) left over) — salvaging what was recoverable"
    )

    {:error, :corrupt, Enum.reverse(acc)}
  end

  @doc """
  Decodes as many complete, CRC-valid frames as possible from the front of
  the binary, unlike `decode_segments/1` this never errors on a truncated or
  corrupt tail — it returns everything decodable plus the undecodable
  remainder, which is what recovering a WAL file after a crash needs (a
  torn last write is expected, not corruption).
  """
  @spec decode_all(binary()) ::
          {[binary()], valid_byte_size :: non_neg_integer(), rest :: binary()}
  def decode_all(binary) when is_binary(binary), do: decode_all(binary, [], 0)

  defp decode_all(
         <<len::32-big, crc::32-big, payload::binary-size(len), rest::binary>> = bin,
         acc,
         valid
       ) do
    if :erlang.crc32(payload) == crc do
      decode_all(rest, [payload | acc], valid + 8 + len)
    else
      {Enum.reverse(acc), valid, bin}
    end
  end

  defp decode_all(rest, acc, valid), do: {Enum.reverse(acc), valid, rest}

  @doc """
  Reads a WAL file, truncating any torn tail in place (a write that never
  finished before a crash). Returns the byte offset at which the next frame
  should be appended. A missing file is treated as an empty log.
  """
  @spec recover!(Path.t()) :: non_neg_integer()
  def recover!(path) do
    case File.read(path) do
      {:ok, contents} ->
        {_payloads, valid, rest} = decode_all(contents)
        if rest != "", do: truncate!(path, valid)
        valid

      {:error, :enoent} ->
        0

      {:error, reason} ->
        # A genuinely failing/corrupted disk (:eio, :eacces, ...), not just a
        # missing file — there's no better recovery than starting from
        # offset 0: whatever was in the file can't be read regardless of how
        # many times this is retried, so treating it as a crash victim (like
        # :enoent) is the only actionable choice. Logged loudly since this is
        # a real disk problem, not routine startup.
        Logger.warning(
          "spool_framing: failed to read #{path} during recovery, starting from offset 0: #{inspect(reason)}"
        )

        0
    end
  end

  defp truncate!(path, valid) do
    {:ok, fd} = :file.open(path, [:read, :write, :raw, :binary])
    {:ok, _} = :file.position(fd, valid)
    :ok = :file.truncate(fd)
    :ok = :file.close(fd)
  end
end
