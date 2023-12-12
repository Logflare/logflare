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

  def convert_primitive("TIMESTAMP", value) do
    trunc(String.to_float(value) * 1_000_000)
  end

  def struct_to_map(struct), do: struct |> Poison.encode!() |> Poison.decode!()

  @spec to_typemap(map | TS.t() | nil) :: %{required(atom) => map | atom}
  def to_typemap(nil), do: nil

  def to_typemap(%TS{} = schema), do: to_typemap(schema, from: :bigquery_schema)

  def to_typemap(metadata) when is_map(metadata) do
    for {k, v} <- metadata, is_not_empty_container?(v), into: Map.new() do
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
        cond do
          is_atom(k) -> k
          String.valid?(k) -> String.to_atom(k)
          true -> decode_until_valid!(k)
        end

      {k, v}
    end
  end

  defp decode_until_valid!(k, encodings \\ [:utf8, :unicode, :latin1])

  defp decode_until_valid!(k, []) do
    raise("Incoming field #{inspect(k)} is not a valid string")
  end

  defp decode_until_valid!(k, [encoding | encodings]) when is_binary(k) do
    case :unicode.characters_to_binary(k, encoding) do
      {:error, _, _} ->
        decode_until_valid!(k, encodings)

      k ->
        k
        |> Unicode.unaccent()
        |> String.to_atom()
    end
  end

  def is_not_empty_container?(value)
      when value == []
      when value == %{}
      when value == [[]]
      when value == [%{}] do
    false
  end

  def is_not_empty_container?(_) do
    true
  end

  @spec to_typemap(TS.t() | list(TS.t()), keyword) :: %{required(atom) => map | atom}
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
    |> to_typemap()
    |> flatten_typemap()
  end

  def flatten_typemap(nil), do: %{}

  def flatten_typemap(%{} = typemap) do
    for {k, v} <- Iteraptor.to_flatmap(typemap), into: %{} do
      {format_flatmap_field_names(k), v}
    end
  end

  defp format_flatmap_field_names(k) do
    String.trim_trailing(k, ".t")
    |> String.replace(".fields.", ".")
  end
end
