defmodule Logflare.SourceSchemas do
  @moduledoc """
  Source schemas in Postgres
  """

  alias Logflare.Repo
  alias Logflare.SourceSchemas.SourceSchema
  alias Logflare.Source.BigQuery.SchemaBuilder

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

  @doc """
  Gets a single source_schema.

  Raises `Ecto.NoResultsError` if the Source schema does not exist.

  ## Examples

      iex> get_source_schema!(123)
      %SourceSchema{}

      iex> get_source_schema!(456)
      ** (Ecto.NoResultsError)

  """
  def get_source_schema!(id), do: Repo.get!(SourceSchema, id)

  def get_source_schema_by(kv), do: SourceSchema |> Repo.get_by(kv)

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

  def find_or_create_source_schema(source) do
    default = %{bigquery_schema: SchemaBuilder.initial_table_schema()}

    case get_source_schema_by(source_id: source.id) do
      nil -> create_source_schema(source, default)
      schema -> {:ok, schema}
    end
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
end
