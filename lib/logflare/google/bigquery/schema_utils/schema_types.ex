defmodule Logflare.BigQuery.SchemaTypes do
  @moduledoc """
  Generates BQ Schema types from values
  """
  def to_schema_type(value) when is_map(value), do: "RECORD"
  def to_schema_type(value) when is_integer(value), do: "INTEGER"
  def to_schema_type(value) when is_binary(value), do: "STRING"
  def to_schema_type(value) when is_boolean(value), do: "BOOLEAN"
  def to_schema_type(value) when is_list(value) and is_map(hd(value)), do: "ARRAY"
  def to_schema_type(value) when is_list(value), do: {"ARRAY", to_schema_type(hd(value))}
  def to_schema_type(value) when is_float(value), do: "FLOAT"

  def to_schema_type({:list, :map}), do: {"ARRAY", "RECORD"}
  def to_schema_type({:list, :integer}), do: {"ARRAY", "INTEGER"}
  def to_schema_type({:list, :string}), do: {"ARRAY", "STRING"}
  def to_schema_type({:list, :boolean}), do: {"ARRAY", "BOOLEAN"}
  def to_schema_type({:list, :float}), do: {"ARRAY", "FLOAT"}
  def to_schema_type({:list, :datetime}), do: {"ARRAY", "DATETIME"}

  def to_schema_type(:map), do: "RECORD"
  def to_schema_type(:integer), do: "INTEGER"
  def to_schema_type(:string), do: "STRING"
  def to_schema_type(:boolean), do: "BOOLEAN"
  def to_schema_type(:list), do: "ARRAY"
  def to_schema_type(:float), do: "FLOAT"
  def to_schema_type(:datetime), do: "DATETIME"

  def bq_type_to_ex("TIMESTAMP"), do: :datetime
  def bq_type_to_ex("RECORD"), do: :map
  def bq_type_to_ex("INTEGER"), do: :integer
  def bq_type_to_ex("STRING"), do: :string
  def bq_type_to_ex("BOOLEAN"), do: :boolean
  def bq_type_to_ex("BOOL"), do: :boolean
  def bq_type_to_ex("ARRAY"), do: :list
  def bq_type_to_ex("FLOAT"), do: :float

  def bq_type_to_ex("ARRAY" <> inner_type) do
    {:list, bq_type_to_ex(String.replace(inner_type, ~r/<|>/, ""))}
  end

  def type_of(arg) when is_binary(arg), do: :string
  def type_of(arg) when is_map(arg), do: :map
  def type_of(arg) when is_list(arg), do: :list
  def type_of(arg) when is_bitstring(arg), do: :bitstring
  def type_of(arg) when is_float(arg), do: :float
  def type_of(arg) when is_function(arg), do: :function
  def type_of(arg) when is_integer(arg), do: :integer
  def type_of(arg) when is_pid(arg), do: :pid
  def type_of(arg) when is_port(arg), do: :port
  def type_of(arg) when is_reference(arg), do: :reference
  def type_of(arg) when is_tuple(arg), do: :tuple
  def type_of(arg) when arg in [true, false], do: :boolean
  def type_of(arg) when is_atom(arg), do: :atom
end
