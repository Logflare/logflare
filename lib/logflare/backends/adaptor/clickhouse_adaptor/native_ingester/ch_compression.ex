defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ChCompression do
  @moduledoc """
  Rust NIF wrapper for ClickHouse native protocol compression primitives.

  Provides CityHash128 v1.0.2 checksums (via `cityhash-rs`) and LZ4 raw block
  compression (via `lz4_flex`). Used together to build the compressed block
  envelope required by the ClickHouse native TCP protocol.

  Note that ClickHouse pins CityHash v1.0.2 specifically — this differs from modern
  CityHash and FarmHash variants.
  """

  use Rustler, otp_app: :logflare, crate: "ch_compression_ex"

  @doc """
  Computes CityHash128 v1.0.2 of the given binary.

  Returns a 16-byte binary in little-endian order (lo64 ++ hi64),
  matching ClickHouse's on-wire checksum format.
  """
  @spec hash128(binary()) :: <<_::128>>
  def hash128(_data), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Compresses data using LZ4 raw block format.
  """
  @spec lz4_compress(binary()) :: {:ok, binary()} | {:error, binary()}
  def lz4_compress(_data), do: :erlang.nif_error(:nif_not_loaded)

  @doc """
  Decompresses LZ4 raw block format data.

  Requires the uncompressed size to allocate the output buffer.
  """
  @spec lz4_decompress(binary(), non_neg_integer()) :: {:ok, binary()} | {:error, binary()}
  def lz4_decompress(_data, _uncompressed_size), do: :erlang.nif_error(:nif_not_loaded)
end
