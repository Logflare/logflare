use std::{collections::HashMap, fmt::Debug, sync::Arc};

use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
use arrow_schema::{DataType as ArrowDataType, Field, Fields, Schema, TimeUnit as ArrowTimeUnit};
use rustler::{Decoder, Encoder, Env, Error, NifResult, OwnedBinary, Term};

mod atoms {
    rustler::atoms! {
        // atoms
        binary,
        boolean,
        category,
        date,
        string,
        time,

        // Tuple
        naive_datetime,
        datetime,
        duration,
        decimal,
        s,
        u,
        f,
        list,
        struct_atom = "struct",

        // TimeUnit
        nanosecond,
        microsecond,
        millisecond,
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum TimeUnit {
    Nanosecond,
    Microsecond,
    Millisecond,
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub enum DType {
    // atom
    Binary,
    Boolean,
    Category,
    Date,
    String,
    Time,

    // tuple
    Datetime(TimeUnit, Option<String>),
    NaiveDatetime(TimeUnit),
    Duration(TimeUnit),
    Decimal(u8, i8),
    F(u8),
    S(u8),
    U(u8),
    List(Box<DType>),
    Struct(Vec<(String, Box<DType>)>),
}

impl<'a> Decoder<'a> for DType {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if term.is_atom() {
            if atoms::binary() == term {
                return Ok(DType::Binary);
            }
            if atoms::boolean() == term {
                return Ok(DType::Boolean);
            }
            if atoms::category() == term {
                return Ok(DType::Category);
            }
            if atoms::date() == term {
                return Ok(DType::Date);
            }
            if atoms::string() == term {
                return Ok(DType::String);
            }
            if atoms::time() == term {
                return Ok(DType::Time);
            }
        }

        if let Ok((tag, p2, p3)) = term.decode::<(Term, Term, Term)>() {
            if atoms::datetime() == tag {
                let unit = TimeUnit::decode(p2)?;
                let timezone: Option<String> = p3.decode()?;
                return Ok(DType::Datetime(unit, timezone));
            } else if atoms::decimal() == tag {
                let precision: u8 = p2.decode()?;
                let scale: i8 = p3.decode()?;
                return Ok(DType::Decimal(precision, scale));
            }
        }

        if let Ok((tag, payload)) = term.decode::<(Term, Term)>() {
            if atoms::s() == tag {
                let width: u8 = payload.decode()?;
                if [8, 16, 32, 64].contains(&width) {
                    return Ok(DType::S(width));
                }
            } else if atoms::u() == tag {
                let width: u8 = payload.decode()?;
                if [8, 16, 32, 64].contains(&width) {
                    return Ok(DType::U(width));
                }
            } else if atoms::f() == tag {
                let width: u8 = payload.decode()?;
                if [32, 64].contains(&width) {
                    return Ok(DType::F(width));
                }
            } else if atoms::duration() == tag {
                return Ok(DType::Duration(TimeUnit::decode(payload)?));
            } else if atoms::naive_datetime() == tag {
                return Ok(DType::NaiveDatetime(TimeUnit::decode(payload)?));
            } else if atoms::list() == tag {
                let msg = format!("{payload:?}");
                let inner_dtype = DType::decode(payload).expect(&msg);
                let inner = Box::new(inner_dtype);
                return Ok(DType::List(inner));
            } else if atoms::struct_atom() == tag {
                let fields: Vec<(String, DType)> = payload.decode().expect("struct ahhhhh");
                let boxed_fields = fields.into_iter().map(|(n, d)| (n, Box::new(d))).collect();
                return Ok(DType::Struct(boxed_fields));
            }
        }

        Err(Error::BadArg)
    }
}

impl<'a> Decoder<'a> for TimeUnit {
    fn decode(term: Term<'a>) -> NifResult<Self> {
        if atoms::nanosecond() == term {
            Ok(TimeUnit::Nanosecond)
        } else if atoms::microsecond() == term {
            Ok(TimeUnit::Microsecond)
        } else if atoms::millisecond() == term {
            Ok(TimeUnit::Millisecond)
        } else {
            Err(Error::BadArg)
        }
    }
}

impl Encoder for DType {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            DType::Binary => atoms::binary().encode(env),
            DType::Boolean => atoms::boolean().encode(env),
            DType::Category => atoms::category().encode(env),
            DType::Date => atoms::date().encode(env),
            DType::String => atoms::string().encode(env),
            DType::Time => atoms::time().encode(env),
            DType::S(width) => (atoms::s(), width).encode(env),
            DType::U(width) => (atoms::u(), width).encode(env),
            DType::F(width) => (atoms::f(), width).encode(env),
            DType::Duration(unit) => (atoms::duration(), unit).encode(env),
            DType::NaiveDatetime(unit) => (atoms::naive_datetime(), unit).encode(env),
            DType::List(inner) => (atoms::list(), inner.as_ref()).encode(env),
            DType::Struct(fields) => {
                let unboxed_fields: Vec<_> = fields
                    .iter()
                    .map(|(n, d)| (n.clone(), d.as_ref()))
                    .collect();
                (atoms::struct_atom(), unboxed_fields).encode(env)
            }
            DType::Datetime(unit, timezone) => (atoms::datetime(), unit, timezone).encode(env),
            DType::Decimal(precision, scale) => (atoms::decimal(), precision, scale).encode(env),
        }
    }
}

impl Encoder for TimeUnit {
    fn encode<'a>(&self, env: Env<'a>) -> Term<'a> {
        match self {
            TimeUnit::Nanosecond => atoms::nanosecond().encode(env),
            TimeUnit::Microsecond => atoms::microsecond().encode(env),
            TimeUnit::Millisecond => atoms::millisecond().encode(env),
        }
    }
}

fn convert_time_unit(tu: &TimeUnit) -> ArrowTimeUnit {
    match tu {
        TimeUnit::Nanosecond => ArrowTimeUnit::Nanosecond,
        TimeUnit::Microsecond => ArrowTimeUnit::Microsecond,
        TimeUnit::Millisecond => ArrowTimeUnit::Millisecond,
    }
}

impl From<DType> for ArrowDataType {
    fn from(dtype: DType) -> Self {
        match dtype {
            DType::Binary => ArrowDataType::Binary,
            DType::Boolean => ArrowDataType::Boolean,
            // Strings but represented internally as integers,
            // TODO utf or int?
            DType::Category => ArrowDataType::Utf8,
            DType::Date => ArrowDataType::Date32,
            DType::Time => ArrowDataType::Time64(ArrowTimeUnit::Nanosecond), // Times are encoded as s64 representing nanoseconds from midnight:
            DType::String => ArrowDataType::Utf8,
            DType::Datetime(tu, tz) => {
                let arrow_tz = tz.map(|s| Arc::from(s.as_str()));
                ArrowDataType::Timestamp(convert_time_unit(&tu), arrow_tz)
            }
            DType::Decimal(precision, scale) => ArrowDataType::Decimal128(precision, scale),
            DType::Duration(tu) => ArrowDataType::Duration(convert_time_unit(&tu)),
            DType::F(32) => ArrowDataType::Float32,
            DType::F(64) => ArrowDataType::Float64,
            DType::List(inner_dtype) => {
                let inner_arrow_type = ArrowDataType::from(*inner_dtype);
                ArrowDataType::new_list(inner_arrow_type, true)
            }
            DType::NaiveDatetime(tu) => ArrowDataType::Timestamp(convert_time_unit(&tu), None),
            DType::S(8) => ArrowDataType::Int8,
            DType::S(16) => ArrowDataType::Int16,
            DType::S(32) => ArrowDataType::Int32,
            DType::S(64) => ArrowDataType::Int64,
            DType::Struct(fields) => {
                // An Arrow Struct contains a list of Fields.
                let arrow_fields: Vec<Field> = fields
                    .into_iter()
                    .map(|(name, dtype)| Field::new(name, ArrowDataType::from(*dtype), true))
                    .collect();
                ArrowDataType::Struct(Fields::from(arrow_fields))
            }
            DType::U(8) => ArrowDataType::UInt8,
            DType::U(16) => ArrowDataType::UInt16,
            DType::U(32) => ArrowDataType::UInt32,
            DType::U(64) => ArrowDataType::UInt64,
            _ => ArrowDataType::Null,
        }
    }
}

#[rustler::nif]
fn serialize_schema<'a>(env: Env<'a>, schema: HashMap<String, DType>) -> NifResult<OwnedBinary> {
    let fields: Vec<Field> = schema
        .into_iter()
        .map(|(name, dtype)| Field::new(name, ArrowDataType::from(dtype), true))
        .collect();

    let schema = Schema::new(fields);

    let options = IpcWriteOptions::default();
    let ipc_generator = IpcDataGenerator::default();
    let mut dictionary_tracker = DictionaryTracker::new(false);

    let encoded_data = ipc_generator.schema_to_bytes_with_dictionary_tracker(
        &schema,
        &mut dictionary_tracker,
        &options,
    );

    let schema_bytes = encoded_data.ipc_message;

    let mut binary = OwnedBinary::new(schema_bytes.len())
        .ok_or_else(|| Error::Term(Box::new("Could not allocate binary")))?;

    binary.as_mut_slice().copy_from_slice(&schema_bytes);

    Ok(binary)
}

rustler::init!("Elixir.Logflare.Backends.Adaptor.BigQueryAdaptor.Arrow.Native");
