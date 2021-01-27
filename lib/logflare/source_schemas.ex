defmodule Logflare.SourceSchemas do
  use Logflare.Commons
  alias Logflare.Google.BigQuery.SchemaUtils
  alias GoogleApi.BigQuery.V2.Model.TableFieldSchema

  @doc """
  Returns the list of source_schemas.

  ## Examples

      iex> list_source_schemas()
      [%SourceSchema{}, ...]

  """
  def list_source_schemas do
    RepoWithCache.all(SourceSchema)
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
  def get_source_schema!(id), do: RepoWithCache.get!(SourceSchema, id)

  def get_source_schema_by(kv), do: SourceSchema |> RepoWithCache.get_by(kv)

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
    |> RepoWithCache.insert()
  end

  def create_source_schema(source, attrs \\ %{}) do
    source
    |> Ecto.build_assoc(:source_schema)
    |> SourceSchema.changeset(attrs)
    |> RepoWithCache.insert()
  end

  def update_source_schema_with_bq_schema_for_source(
        %TableFieldSchema{} = bq_schema,
        %Source{} = source
      ) do
    bq_schema = SchemaUtils.deep_sort_by_fields_name(bq_schema)
    type_map = SchemaUtils.to_typemap(bq_schema)
    field_count = SchemaUtils.count_fields(type_map)

    source_schema = get_source_schema_by(source_id: source.id)

    update_source_schema(source_schema, %{
      bigquery_schema: bq_schema,
      field_count: field_count,
      type_map: type_map
    })
  end

  def update_source_schema(%SourceSchema{} = source_schema, attrs) do
    source_schema
    |> SourceSchema.changeset(attrs)
    |> RepoWithCache.update()
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
    RepoWithCache.delete(source_schema)
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

  def create_or_update_source_schema_for_source(attrs, %Source{} = source) do
  end

  def get_source_schema!(id), do: RepoWithCache.get!(SourceSchema, id)

  def get_source_schema_by(kv), do: SourceSchema |> RepoWithCache.get_by(kv)
end
