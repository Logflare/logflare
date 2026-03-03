defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.Compression do
  @moduledoc """
  Builds ClickHouse compressed block envelopes for the native TCP protocol.

  Composes CityHash128 v1.0.2 checksums and LZ4 compression from the
  `ChCompression` NIF into the envelope format expected by ClickHouse.
  """

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ChCompression

  @lz4_method 0x82
  @header_size 9

  @doc """
  Compresses a raw data block into the ClickHouse compressed block envelope.

  Takes iodata (typically from `BlockEncoder`), LZ4-compresses it, and wraps
  it in the full envelope with CityHash128 checksum.
  """
  @spec compress(iodata()) :: binary()
  def compress(block_iodata) do
    raw = IO.iodata_to_binary(block_iodata)
    {:ok, compressed_data} = ChCompression.lz4_compress(raw)

    uncompressed_size = byte_size(raw)
    compressed_size = @header_size + byte_size(compressed_data)

    header_and_data =
      <<@lz4_method::8, compressed_size::little-unsigned-32,
        uncompressed_size::little-unsigned-32, compressed_data::binary>>

    checksum = ChCompression.hash128(header_and_data)

    <<checksum::binary-size(16), header_and_data::binary>>
  end

  @doc """
  Decompresses a ClickHouse compressed block envelope.

  Verifies the CityHash128 checksum, then decompresses the LZ4 payload.
  Returns `{:ok, decompressed}` or `{:error, reason}`.
  """
  @spec decompress(binary()) :: {:ok, binary()} | {:error, term()}
  def decompress(
        <<checksum::binary-size(16), @lz4_method::8, compressed_size::little-unsigned-32,
          uncompressed_size::little-unsigned-32, rest::binary>>
      ) do
    data_size = compressed_size - @header_size
    <<compressed_data::binary-size(data_size), _rest::binary>> = rest

    header_and_data =
      <<@lz4_method::8, compressed_size::little-unsigned-32,
        uncompressed_size::little-unsigned-32, compressed_data::binary>>

    expected_checksum = ChCompression.hash128(header_and_data)

    if checksum == expected_checksum do
      ChCompression.lz4_decompress(compressed_data, uncompressed_size)
    else
      {:error, :checksum_mismatch}
    end
  end

  def decompress(<<_checksum::binary-size(16), method::8, _rest::binary>>) do
    {:error, {:unsupported_compression_method, method}}
  end

  def decompress(_data), do: {:error, :invalid_compressed_block}
end
