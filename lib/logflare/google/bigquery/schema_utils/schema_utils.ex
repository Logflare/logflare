defmodule Logflare.Google.BigQuery.SchemaUtils do
  @doc """
  Transform BigQuery query response into a map.
  via https://stackoverflow.com/questions/53913182/decoding-googleapi-bigquery-object-elixir
  """

  @empty [nil, %{}, []]

  def merge_rows_with_schema(nil, nil), do: []
  def merge_rows_with_schema(%GoogleApi.BigQuery.V2.Model.TableSchema{} = _schema, nil), do: []

  def merge_rows_with_schema(%GoogleApi.BigQuery.V2.Model.TableSchema{} = schema, rows) do
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

  def convert_primtive("TIMESTAMP", value) do
    (String.to_float(value) * 1_000_000)
    |> trunc
  end

  def struct_to_map(struct), do: struct |> Poison.encode!() |> Poison.decode!()
end
