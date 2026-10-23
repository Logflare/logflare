//! Types shared by every serialized mapper output writer (RowBinary, NDJSON):
//! the per-row envelope, the error alias, integer decoding, and an
//! `OwnedBinary`-backed growable buffer.

use rustler::{Binary, OwnedBinary, Term};

pub type EncodeResult<T> = Result<T, String>;

const INITIAL_ROW_CAPACITY: usize = 3072;

/// Per-row values supplied by the caller alongside the mapped document.
#[derive(Clone, Copy)]
pub struct RowEnvelope<'a> {
    pub id: Binary<'a>,
    pub source_uuid: Binary<'a>,
    pub source_name: Binary<'a>,
    pub ingested_at: i64,
}

pub fn decode_u64(value: Term, field: &str) -> EncodeResult<u64> {
    if let Ok(value) = value.decode::<u64>() {
        return Ok(value);
    }
    match value.decode::<i64>() {
        Ok(value) => u64::try_from(value).map_err(|_| format!("mapped field {field} is negative")),
        Err(_) => Err(format!("mapped field {field} is not an integer")),
    }
}

/// Grows an `OwnedBinary` in place so a row is written straight into the
/// binary handed back to the VM, with no intermediate copy.
pub struct BinaryBuilder {
    binary: OwnedBinary,
    len: usize,
}

impl BinaryBuilder {
    pub fn new() -> EncodeResult<Self> {
        let binary = OwnedBinary::new(INITIAL_ROW_CAPACITY)
            .ok_or_else(|| "failed to allocate row output".to_string())?;
        Ok(Self { binary, len: 0 })
    }

    pub fn finish(mut self) -> EncodeResult<OwnedBinary> {
        if self.len == 0 {
            return OwnedBinary::new(0)
                .ok_or_else(|| "failed to allocate empty row output".to_string());
        }
        self.resize(self.len)?;
        Ok(self.binary)
    }

    pub fn push(&mut self, value: u8) -> EncodeResult<()> {
        let end = self.reserve(1)?;
        self.binary.as_mut_slice()[self.len] = value;
        self.len = end;
        Ok(())
    }

    pub fn extend_from_slice(&mut self, value: &[u8]) -> EncodeResult<()> {
        let end = self.reserve(value.len())?;
        self.binary.as_mut_slice()[self.len..end].copy_from_slice(value);
        self.len = end;
        Ok(())
    }

    fn reserve(&mut self, additional: usize) -> EncodeResult<usize> {
        let required = self
            .len
            .checked_add(additional)
            .ok_or_else(|| "row output size overflow".to_string())?;
        if required <= self.binary.len() {
            return Ok(required);
        }

        let capacity = self
            .binary
            .len()
            .saturating_mul(2)
            .max(required)
            .max(INITIAL_ROW_CAPACITY);
        self.resize(capacity)?;
        Ok(required)
    }

    fn resize(&mut self, size: usize) -> EncodeResult<()> {
        if self.binary.realloc(size) {
            return Ok(());
        }

        let copy_len = self.len.min(size);
        let mut replacement =
            OwnedBinary::new(size).ok_or_else(|| "failed to resize row output".to_string())?;
        let initialized = &self.binary.as_mut_slice()[..copy_len];
        replacement.as_mut_slice()[..copy_len].copy_from_slice(initialized);
        self.binary = replacement;
        Ok(())
    }
}
