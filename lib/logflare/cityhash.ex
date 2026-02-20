defmodule Logflare.CityHash do
  @moduledoc """
  Rust NIF wrapper for CityHash v1.0.2 (via `cityhash-rs`).

  Used by the ClickHouse native TCP protocol for compression block checksums.
  ClickHouse pins CityHash v1.0.2 specifically — this differs from modern
  CityHash and FarmHash variants.
  """

  use Rustler, otp_app: :logflare, crate: "cityhash_ex"

  @doc """
  Computes CityHash128 v1.0.2 of the given binary.

  Returns a 16-byte binary in little-endian order (lo64 ++ hi64),
  matching ClickHouse's on-wire checksum format.
  """
  @spec hash128(binary()) :: <<_::128>>
  def hash128(_data), do: :erlang.nif_error(:nif_not_loaded)
end
