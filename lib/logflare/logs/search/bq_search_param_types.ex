defmodule Logflare.Logs.BigQuery.SearchParamTypes do
  @moduledoc false

  def to_schema_type(value) when is_map(value), do: "RECORD"
  def to_schema_type(value) when is_integer(value), do: "INTEGER"
  def to_schema_type(value) when is_binary(value), do: "STRING"
  def to_schema_type(value) when is_boolean(value), do: "BOOL"
  def to_schema_type(value) when is_list(value), do: "ARRAY"
  def to_schema_type(value) when is_float(value), do: "FLOAT"

  def to_schema_type(:NULL), do: "NULL"
  def to_schema_type(:map), do: "RECORD"
  def to_schema_type(:integer), do: "INTEGER"
  def to_schema_type(:string), do: "STRING"
  def to_schema_type(:boolean), do: "BOOL"
  def to_schema_type(:list), do: "ARRAY"
  def to_schema_type(:float), do: "FLOAT"
  def to_schema_type(:datetime), do: "DATETIME"
end
