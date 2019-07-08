defmodule Logflare.BigQuery.SchemaTypes do
  @moduledoc """
  Generates BQ Schema types from values
  """
  def to_schema_type(value) when is_map(value), do: "RECORD"
  def to_schema_type(value) when is_integer(value), do: "INTEGER"
  def to_schema_type(value) when is_binary(value), do: "STRING"
  def to_schema_type(value) when is_boolean(value), do: "BOOLEAN"
  def to_schema_type(value) when is_list(value), do: "ARRAY"
  def to_schema_type(value) when is_float(value), do: "FLOAT"
end
