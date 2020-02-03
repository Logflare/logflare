defmodule Logflare.Google.BigQuery.TestUtils do
  @moduledoc false
  import ExUnit.Assertions

  @doc """
  Utility function for removing everything except schemas names from TableFieldSchema structs
  for easier debugging of errors when not all fields schemas are present in the result
  """
  def deep_schema_to_field_names(fields) when is_list(fields) do
    Enum.map(fields, &deep_schema_to_field_names/1)
  end

  def deep_schema_to_field_names(%{fields: fields} = schema) when is_list(fields) do
    %{
      Map.get(schema, :name, :top_level_schema) => Enum.map(fields, &deep_schema_to_field_names/1)
    }
  end

  def deep_schema_to_field_names(%{name: name}) do
    name
  end

  def assert_equal_schemas(schema_left, schema_right) do
    assert deep_schema_to_field_names(schema_left) == deep_schema_to_field_names(schema_right)
    assert schema_left == schema_right
  end
end
