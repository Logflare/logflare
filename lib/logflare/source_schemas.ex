defmodule Logflare.SourceSchemas do
  @moduledoc """
  Source schemas in Postgres
  """

  alias Logflare.Repo
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Google.BigQuery.SchemaUtils

  require Logger

  @doc """
  Returns the list of source_schemas.

  ## Examples

      iex> list_source_schemas()
      [%SourceSchema{}, ...]

  """
  def list_source_schemas do
    Repo.all(SourceSchema)
  end

  def get_source_schema(id) do
    Repo.get(SourceSchema, id)
  end

  def get_source_schema_by(kv) do
    SourceSchema |> Repo.get_by(kv)
  end

  @doc """
  Creates a source_schema.

  ## Examples

      iex> create_source_schema(%{field: value})
      {:ok, %SourceSchema{}}

      iex> create_source_schema(%{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def create_source_schema(source, attrs \\ %{}) do
    source
    |> Ecto.build_assoc(:source_schema)
    |> SourceSchema.changeset(attrs)
    |> Repo.insert()
  end

  def create_or_update_source_schema(source, attrs) do
    case get_source_schema_by(source_id: source.id) do
      nil -> create_source_schema(source, attrs)
      source_schema -> update_source_schema(source_schema, attrs)
    end
  end

  @doc """
  Updates a source_schema.

  ## Examples

      iex> update_source_schema(source_schema, %{field: new_value})
      {:ok, %SourceSchema{}}

      iex> update_source_schema(source_schema, %{field: bad_value})
      {:error, %Ecto.Changeset{}}

  """
  def update_source_schema(%SourceSchema{} = source_schema, attrs) do
    source_schema
    |> SourceSchema.changeset(attrs)
    |> Repo.update()
  end

  @doc """
  Deletes a source_schema.

  ## Examples

      iex> delete_source_schema(source_schema)
      {:ok, %SourceSchema{}}

      iex> delete_source_schema(source_schema)
      {:error, %Ecto.Changeset{}}

  """
  def delete_source_schema(%SourceSchema{} = source_schema) do
    Repo.delete(source_schema)
  end

  @doc """
  Returns an `%Ecto.Changeset{}` for tracking source_schema changes.

  ## Examples

      iex> change_source_schema(source_schema)
      %Ecto.Changeset{data: %SourceSchema{}}

  """
  def change_source_schema(%SourceSchema{} = source_schema, attrs \\ %{}) do
    SourceSchema.changeset(source_schema, attrs)
  end

  def format_schema(bq_schema, variant, to_merge \\ %{})

  def format_schema(%SourceSchema{bigquery_schema: bq_schema} = schema, :dot, to_merge) do
    bq_schema
    |> SchemaUtils.bq_schema_to_flat_typemap()
    |> Enum.filter(fn
      {_k, :map} -> false
      _ -> true
    end)
    |> Enum.map(fn
      {k, {:list, type}} -> {k, "#{type}[]"}
      {k, v} -> {k, Atom.to_string(v)}
    end)
    |> Map.new()
    |> Map.merge(to_merge)
  end

  def format_schema(%SourceSchema{bigquery_schema: bq_schema}, :json_schema, to_merge) do
    bq_schema
    |> SchemaUtils.to_typemap()
    |> typemap_to_json_schema()
    |> Map.merge(to_merge)
    |> Map.put(
      "$schema",
      "https://json-schema.org/draft/2020-12/schema"
    )
  end

  defp typemap_to_json_schema(map) when is_map(map) do
    properties = Enum.map(map, &typemap_to_json_schema/1) |> Map.new()

    %{
      "properties" => properties,
      "type" => "object"
    }
  end

  defp typemap_to_json_schema({key, %{fields: fields, t: :map}}) do
    {Atom.to_string(key), typemap_to_json_schema(fields)}
  end

  defp typemap_to_json_schema({key, %{t: {:list, type}}}) do
    {Atom.to_string(key), %{"type" => "array", "items" => %{"type" => Atom.to_string(type)}}}
  end

  defp typemap_to_json_schema({key, %{t: :datetime}}),
    do: {Atom.to_string(key), %{"type" => "number"}}

  defp typemap_to_json_schema({key, %{t: :integer}}),
    do: {Atom.to_string(key), %{"type" => "number"}}

  defp typemap_to_json_schema({key, %{t: :float}}),
    do: {Atom.to_string(key), %{"type" => "number"}}

  defp typemap_to_json_schema({key, %{t: type}}),
    do: {Atom.to_string(key), %{"type" => Atom.to_string(type)}}
end
