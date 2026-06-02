defmodule Logflare.Google.BigQuery.SchemaUtils do
  @moduledoc """
  Various utility functions for BQ Schemas
  """

  require Logger
  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS
  import Logflare.BigQuery.SchemaTypes

  @doc """
  Transform BigQuery query response into a map.
  via https://stackoverflow.com/questions/53913182/decoding-googleapi-bigquery-object-elixir
  """
  @spec deep_sort_by_fields_name(TS.t() | TFS.t()) :: TS.t() | TFS.t()
  def deep_sort_by_fields_name(%{fields: nil} = schema), do: schema

  def deep_sort_by_fields_name(%{fields: fields} = schema) when is_list(fields) do
    sorted_fields =
      fields
      |> Enum.sort_by(& &1.name)
      |> Enum.map(&deep_sort_by_fields_name/1)

    %{schema | fields: sorted_fields}
  end

  @empty [nil, %{}, []]

  def merge_rows_with_schema(nil, nil), do: []
  def merge_rows_with_schema(%TS{} = _schema, nil), do: []

  def merge_rows_with_schema(%TS{} = schema, rows) do
    rows |> struct_to_map() |> Enum.map(&merge_rows_with_schema_(schema.fields, &1["f"]))
  end

  defp merge_rows_with_schema_(_schema, fields) when fields in @empty, do: []

  defp merge_rows_with_schema_(schema, fields) do
    schema
    |> Enum.zip_with(fields, &merge_row/2)
    |> Map.new()
  end

  def merge_row(schema_field, field) do
    converted_val = convert(schema_field.mode, schema_field.type, schema_field, field)
    {schema_field.name, converted_val}
  end

  def convert(_mode, _type, _schema, value) when value in @empty, do: nil

  def convert("REPEATED", type, schema, field),
    do: field["v"] |> Enum.map(&convert(nil, type, schema, &1))

  def convert(_mode, "RECORD", schema, field),
    do: merge_rows_with_schema_(schema.fields, field["v"]["f"])

  def convert(_mode, _type, schema, field), do: convert_primitive(schema.type, field["v"])

  def convert_primitive(_type, value) when value in @empty, do: nil
  def convert_primitive("STRING", value), do: value
  def convert_primitive("BOOLEAN", value), do: value == "true"
  def convert_primitive("BOOL", value), do: value == "true"
  def convert_primitive("FLOAT", value), do: String.to_float(value)
  def convert_primitive("INTEGER", value), do: String.to_integer(value)
  def convert_primitive("DATE", value), do: value
  def convert_primitive("DATETIME", value), do: value
  def convert_primitive("JSON", value), do: value

  def convert_primitive("TIMESTAMP", value) do
    trunc(String.to_float(value) * 1_000_000)
  end

  def struct_to_map(struct), do: struct |> Poison.encode!() |> Poison.decode!()

  @spec to_typemap(TS.t() | [TFS.t()], keyword) :: %{required(atom) => map | atom}
  def to_typemap(%TS{fields: fields} = schema, from: :bigquery_schema) when is_map(schema) do
    to_typemap(fields, from: :bigquery_schema)
  end

  def to_typemap(fields, from: :bigquery_schema) when is_list(fields) do
    Map.new(fields, fn
      %TFS{fields: fields, name: n, type: t, mode: mode} ->
        k = String.to_atom(n)

        v =
          cond do
            mode == "REPEATED" and t == "RECORD" ->
              %{t: bq_type_to_ex(t)}

            mode == "REPEATED" and t != "RECORD" ->
              %{t: {:list, bq_type_to_ex(t)}}

            mode in ["NULLABLE", "REQUIRED"] ->
              %{t: bq_type_to_ex(t)}

            is_nil(mode) ->
              %{t: bq_type_to_ex(t)}

            true ->
              Logger.warning("Unexpected value of TFS mode: #{mode}")
              %{t: bq_type_to_ex(t)}
          end

        v =
          if fields do
            Map.put(v, :fields, to_typemap(fields, from: :bigquery_schema))
          else
            v
          end

        {k, v}
    end)
  end

  @spec bq_schema_to_flat_typemap(TS.t() | nil) :: map
  def bq_schema_to_flat_typemap(nil), do: %{}

  def bq_schema_to_flat_typemap(%TS{} = schema) do
    schema
    |> to_typemap(from: :bigquery_schema)
    |> flatten_typemap()
  end

  @spec flatten_typemap(map | nil) :: map
  def flatten_typemap(nil), do: %{}

  def flatten_typemap(typemap) when is_map(typemap) do
    do_flatten_typemap(typemap, "", %{})
  end

  defp do_flatten_typemap(typemap, prefix, acc) do
    Enum.reduce(typemap, acc, fn {key, value}, acc ->
      flatten_node(value, join_key(prefix, key), acc)
    end)
  end

  defp join_key("", key), do: to_string(key)
  defp join_key(prefix, key), do: prefix <> "." <> to_string(key)

  defp flatten_node(%{t: :map, fields: fields}, key, acc) do
    do_flatten_typemap(fields, key, Map.put(acc, key, :map))
  end

  defp flatten_node(%{t: type}, key, acc) do
    Map.put(acc, key, type)
  end

  @spec get_type_for_path([term()], map()) :: atom() | nil
  def get_type_for_path(path, flat_schema_map) when is_list(path) and is_map(flat_schema_map) do
    path
    |> schema_keys_for_path()
    |> Enum.find_value(fn key -> Map.get(flat_schema_map, key) end)
  end

  def get_type_for_path(_path, _flat_schema_map), do: nil

  defp schema_keys_for_path(path) do
    direct_key = path |> Enum.map(&to_string/1) |> Enum.join(".")

    without_array_indexes =
      path
      |> Enum.reject(&array_index?/1)
      |> Enum.map(&to_string/1)
      |> Enum.join(".")

    [direct_key, without_array_indexes]
    |> Enum.reject(&(&1 == ""))
    |> Enum.uniq()
  end

  defp array_index?(segment) when is_integer(segment), do: true

  defp array_index?(segment) do
    case Integer.parse(to_string(segment)) do
      {_index, ""} -> true
      _ -> false
    end
  end
end
