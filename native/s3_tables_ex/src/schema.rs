use std::sync::Arc;

use iceberg::spec::{ListType, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, Type};
use rustler::NifMap;

/// Mirrors `Logflare.Backends.Adaptor.S3TablesAdaptor.IcebergSchema.field()` on the Elixir side.
#[derive(Debug, NifMap)]
pub struct FieldSpec {
    pub name: String,
    pub r#type: String,
    pub required: bool,
}

/// Builds an Iceberg schema from the Elixir-defined field list, assigning sequential field
/// IDs. `TableMetadataBuilder::from_table_creation` reassigns all field IDs on create, so
/// these only need to be unique within the schema being built here.
///
/// Returns the schema along with the field ID of the required `timestamp` column, used to
/// build the day-partition spec.
pub fn build(fields: &[FieldSpec]) -> Result<(Schema, i32), String> {
    let mut next_id = 0;
    let mut nested_fields: Vec<NestedFieldRef> = Vec::with_capacity(fields.len());
    let mut timestamp_field_id = None;

    for field in fields {
        let id = allocate_id(&mut next_id);
        let field_type = parse_type(&field.r#type, &mut next_id)?;

        if field.name == "timestamp" {
            timestamp_field_id = Some(id);
        }

        let nested = if field.required {
            NestedField::required(id, &field.name, field_type)
        } else {
            NestedField::optional(id, &field.name, field_type)
        };

        nested_fields.push(Arc::new(nested));
    }

    let timestamp_field_id = timestamp_field_id
        .ok_or_else(|| "schema is missing a required \"timestamp\" field".to_string())?;

    let schema = Schema::builder()
        .with_fields(nested_fields)
        .build()
        .map_err(|err| format!("{err:?}"))?;

    Ok((schema, timestamp_field_id))
}

fn allocate_id(next_id: &mut i32) -> i32 {
    *next_id += 1;
    *next_id
}

fn parse_type(dsl: &str, next_id: &mut i32) -> Result<Type, String> {
    match dsl {
        "string" => Ok(Type::Primitive(PrimitiveType::String)),
        "int" => Ok(Type::Primitive(PrimitiveType::Int)),
        "long" => Ok(Type::Primitive(PrimitiveType::Long)),
        "double" => Ok(Type::Primitive(PrimitiveType::Double)),
        "boolean" => Ok(Type::Primitive(PrimitiveType::Boolean)),
        "timestamptz" => Ok(Type::Primitive(PrimitiveType::Timestamptz)),
        "map<string,string>" => Ok(string_map_type(next_id)),
        "list<long>" => Ok(list_type(next_id, Type::Primitive(PrimitiveType::Long))),
        "list<double>" => Ok(list_type(next_id, Type::Primitive(PrimitiveType::Double))),
        "list<string>" => Ok(list_type(next_id, Type::Primitive(PrimitiveType::String))),
        "list<timestamptz>" => Ok(list_type(
            next_id,
            Type::Primitive(PrimitiveType::Timestamptz),
        )),
        "list<map<string,string>>" => {
            let map_type = string_map_type(next_id);
            Ok(list_type(next_id, map_type))
        }
        other => Err(format!("unknown iceberg schema DSL type: {other}")),
    }
}

fn string_map_type(next_id: &mut i32) -> Type {
    let key_id = allocate_id(next_id);
    let value_id = allocate_id(next_id);

    Type::Map(MapType::new(
        Arc::new(NestedField::map_key_element(
            key_id,
            Type::Primitive(PrimitiveType::String),
        )),
        Arc::new(NestedField::map_value_element(
            value_id,
            Type::Primitive(PrimitiveType::String),
            true,
        )),
    ))
}

fn list_type(next_id: &mut i32, element_type: Type) -> Type {
    let element_id = allocate_id(next_id);

    Type::List(ListType::new(Arc::new(NestedField::list_element(
        element_id,
        element_type,
        true,
    ))))
}
