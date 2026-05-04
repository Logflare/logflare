/// String filters for validating resolved values during path coalescing.
///
/// Filters are checked after a string value is resolved: if the value
/// doesn't pass, it's treated as "not found" and the next path is tried.
/// All configured filters must pass (AND logic).

#[derive(Debug)]
pub struct StringFilters {
    pub len_eq: Option<usize>,
    pub len_gt: Option<usize>,
    pub len_gte: Option<usize>,
    pub len_lt: Option<usize>,
    pub len_lte: Option<usize>,
    pub char_class: Option<CharClass>,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CharClass {
    Alpha,
    Numeric,
    Alphanumeric,
}

/// Check if a byte slice passes all configured filters.
///
/// Length checks are O(1) and run first, gating the more expensive
/// char class check which is O(n) with early exit.
#[inline]
pub fn passes_filters(bytes: &[u8], filters: &StringFilters) -> bool {
    let len = bytes.len();

    if let Some(eq) = filters.len_eq {
        if len != eq {
            return false;
        }
    }
    if let Some(gt) = filters.len_gt {
        if len <= gt {
            return false;
        }
    }
    if let Some(gte) = filters.len_gte {
        if len < gte {
            return false;
        }
    }
    if let Some(lt) = filters.len_lt {
        if len >= lt {
            return false;
        }
    }
    if let Some(lte) = filters.len_lte {
        if len > lte {
            return false;
        }
    }

    if let Some(ref class) = filters.char_class {
        match class {
            CharClass::Alpha => {
                if !bytes.iter().all(|b| b.is_ascii_alphabetic()) {
                    return false;
                }
            }
            CharClass::Numeric => {
                if !bytes.iter().all(|b| b.is_ascii_digit()) {
                    return false;
                }
            }
            CharClass::Alphanumeric => {
                if !bytes.iter().all(|b| b.is_ascii_alphanumeric()) {
                    return false;
                }
            }
        }
    }

    true
}
