use cityhash_rs::cityhash_102_128;
use rustler::{Atom, Binary, Env, NewBinary};

mod atoms {
    rustler::atoms! {
        ok,
        error,
    }
}

/// Computes CityHash128 v1.0.2 of the input binary.
///
/// Returns a 16-byte binary in little-endian order (lo64 ++ hi64),
/// matching ClickHouse's on-wire representation for compression checksums.
#[rustler::nif]
fn hash128<'a>(env: Env<'a>, data: Binary) -> Binary<'a> {
    let hash: u128 = cityhash_102_128(data.as_slice());

    // cityhash-rs packs (first, second) with first in the high bits of u128.
    // ClickHouse writes first (low64) at offset 0, second (high64) at offset 8.
    let lo: u64 = (hash >> 64) as u64;
    let hi: u64 = hash as u64;

    let mut output = NewBinary::new(env, 16);
    output.as_mut_slice()[..8].copy_from_slice(&lo.to_le_bytes());
    output.as_mut_slice()[8..].copy_from_slice(&hi.to_le_bytes());

    output.into()
}

/// Compresses data using LZ4 raw block format.
///
/// Returns `{:ok, compressed_binary}` or `{:error, reason}`.
#[rustler::nif]
fn lz4_compress<'a>(env: Env<'a>, data: Binary) -> (Atom, Binary<'a>) {
    let compressed = lz4_flex::compress_prepend_size(data.as_slice());

    // lz4_flex::compress_prepend_size prepends a 4-byte LE size header.
    // ClickHouse uses raw LZ4 blocks without this header, so strip it.
    let raw = &compressed[4..];

    let mut output = NewBinary::new(env, raw.len());
    output.as_mut_slice().copy_from_slice(raw);
    (atoms::ok(), output.into())
}

/// Decompresses LZ4 raw block format data.
///
/// Requires the uncompressed size to allocate the output buffer.
/// Returns `{:ok, decompressed_binary}` or `{:error, reason}`.
#[rustler::nif]
fn lz4_decompress<'a>(env: Env<'a>, data: Binary, uncompressed_size: usize) -> (Atom, Binary<'a>) {
    match lz4_flex::decompress(data.as_slice(), uncompressed_size) {
        Ok(decompressed) => {
            let mut output = NewBinary::new(env, decompressed.len());
            output.as_mut_slice().copy_from_slice(&decompressed);
            (atoms::ok(), output.into())
        }
        Err(e) => {
            let msg = format!("{}", e);
            let mut output = NewBinary::new(env, msg.len());
            output.as_mut_slice().copy_from_slice(msg.as_bytes());
            (atoms::error(), output.into())
        }
    }
}

rustler::init!("Elixir.Logflare.Backends.Adaptor.ClickHouseAdaptor.NativeIngester.ChCompression");
