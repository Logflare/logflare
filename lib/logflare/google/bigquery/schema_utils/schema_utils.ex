defmodule Logflare.Google.BigQuery.SchemaUtils do
  @moduledoc """
  Various utility functions for BQ Schemas
  """

  alias GoogleApi.BigQuery.V2.Model.TableSchema, as: TS
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema, as: TFS

  @doc """
  Transform BigQuery query response into a map.
  via https://stackoverflow.com/questions/53913182/decoding-googleapi-bigquery-object-elixir
  """
  @spec deep_sort_by_fields_name(TFS.t()) :: TFS.t()
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
    rows |> struct_to_map |> Enum.map(&merge_rows_with_schema_(schema.fields, &1["f"]))
  end

  defp merge_rows_with_schema_(_schema, fields) when fields in @empty, do: []

  defp merge_rows_with_schema_(schema, fields) do
    fields
    |> Stream.with_index()
    |> Enum.reduce([], fn {field, i}, acc -> [merge_row(Enum.at(schema, i), field)] ++ acc end)
    |> Enum.into(%{})
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

  def convert(_mode, _type, schema, field), do: convert_primtive(schema.type, field["v"])

  def convert_primtive(_type, value) when value in @empty, do: nil
  def convert_primtive("STRING", value), do: value
  def convert_primtive("BOOLEAN", value), do: value == "true"
  def convert_primtive("BOOL", value), do: value == "true"
  def convert_primtive("FLOAT", value), do: String.to_float(value)
  def convert_primtive("INTEGER", value), do: String.to_integer(value)
  def convert_primtive("DATETIME", value), do: value

  def convert_primtive("TIMESTAMP", value) do
    (String.to_float(value) * 1_000_000)
    |> trunc
  end

  def struct_to_map(struct), do: struct |> Poison.encode!() |> Poison.decode!()

  @spec to_typemap(map | TS.t() | nil) :: %{required(atom) => map | atom}
  def to_typemap(nil), do: nil

  def to_typemap(%TS{} = schema), do: to_typemap(schema, from: :bigquery_schema)

  def to_typemap(metadata) when is_map(metadata) do
    for {k, v} <- metadata, into: Map.new() do
      v =
        cond do
          match?(%DateTime{}, v) or
              match?(%NaiveDateTime{}, v) ->
            %{t: :datetime}

          is_list(v) and is_map(hd(v)) ->
            %{
              t: :map,
              fields: Enum.reduce(v, %{}, &Map.merge(&2, to_typemap(&1)))
            }

          is_list(v) and not is_map(hd(v)) ->
            %{t: {:list, type_of(hd(v))}}

          is_map(v) ->
            %{t: :map, fields: to_typemap(v)}

          true ->
            %{t: type_of(v)}
        end

      k =
        if is_atom(k) do
          k
        else
          String.to_existing_atom(k)
        end

      {k, v}
    end
  end

  @spec to_typemap(TS.t() | list(TS.t()), keyword) :: %{required(atom) => map | atom}
  def to_typemap(%TS{fields: fields} = schema, from: :bigquery_schema) when is_map(schema) do
    to_typemap(fields, from: :bigquery_schema)
  end

  def to_typemap(fields, from: :bigquery_schema) when is_list(fields) do
    fields
    |> Enum.map(fn
      %TFS{fields: fields, name: n, type: t, mode: mode} ->
        k = String.to_atom(n)

        v =
          cond do
            mode == "REPEATED" and t == "RECORD" -> %{t: bq_type_to_ex(t)}
            mode == "REPEATED" and t != "RECORD" -> %{t: {:list, bq_type_to_ex(t)}}
            mode in ["NULLABLE", "REQUIRED"] -> %{t: bq_type_to_ex(t)}
          end

        v =
          if fields do
            Map.put(v, :fields, to_typemap(fields, from: :bigquery_schema))
          else
            v
          end

        {k, v}
    end)
    |> Map.new()
  end

  def bq_type_to_ex("TIMESTAMP"), do: :datetime
  def bq_type_to_ex("RECORD"), do: :map
  def bq_type_to_ex("INTEGER"), do: :integer
  def bq_type_to_ex("STRING"), do: :string
  def bq_type_to_ex("BOOLEAN"), do: :boolean
  def bq_type_to_ex("BOOL"), do: :boolean
  def bq_type_to_ex("ARRAY"), do: :list
  def bq_type_to_ex("FLOAT"), do: :float

  defp type_of(arg) when is_binary(arg), do: :string
  defp type_of(arg) when is_map(arg), do: :map
  defp type_of(arg) when is_list(arg), do: :list
  defp type_of(arg) when is_bitstring(arg), do: :bitstring
  defp type_of(arg) when is_float(arg), do: :float
  defp type_of(arg) when is_function(arg), do: :function
  defp type_of(arg) when is_integer(arg), do: :integer
  defp type_of(arg) when is_pid(arg), do: :pid
  defp type_of(arg) when is_port(arg), do: :port
  defp type_of(arg) when is_reference(arg), do: :reference
  defp type_of(arg) when is_tuple(arg), do: :tuple
  defp type_of(arg) when arg in [true, false], do: :boolean
  defp type_of(arg) when is_atom(arg), do: :atom
end
