use cityhash_rs::cityhash_102_128;
use rustler::{Binary, Env, NewBinary};

/// Computes CityHash128 v1.0.2 of the input binary.
///
/// Returns a 16-byte binary in little-endian order (lo64 ++ hi64),
/// matching ClickHouse's on-wire representation for compression checksums.
#[rustler::nif]
fn hash128<'a>(env: Env<'a>, data: Binary) -> Binary<'a> {
    let hash: u128 = cityhash_102_128(data.as_slice());

    let lo: u64 = hash as u64;
    let hi: u64 = (hash >> 64) as u64;

    let mut output = NewBinary::new(env, 16);
    output.as_mut_slice()[..8].copy_from_slice(&lo.to_le_bytes());
    output.as_mut_slice()[8..].copy_from_slice(&hi.to_le_bytes());

    output.into()
}

rustler::init!("Elixir.Logflare.CityHash");
