defmodule Logflare.SourceSchemas do
  use Logflare.Commons
  alias Logflare.Sources.SourceSchema

  @spec list_source_schemas() :: [SourceSchema.t()]
  def list_source_schemas do
    RepoWithCache.all(SourceSchema)
  end

  @spec get_source_schema!(integer()) :: SourceSchema.t() | nil
  def get_source_schema!(id), do: RepoWithCache.get!(SourceSchema, id)

  @spec get_source_schema_by(Keyword.t()) :: SourceSchema.t() | nil
  def get_source_schema_by(kv), do: SourceSchema |> RepoWithCache.get_by(kv)

  @spec create_source_schema_for_source(map(), Source.t()) ::
          {:ok, SourceSchema.t()} | {:error, term()}
  def create_source_schema_for_source(attrs \\ %{}, %Source{} = source) do
    source
    |> Ecto.build_assoc(:source_schema)
    |> SourceSchema.changeset(attrs)
    |> RepoWithCache.insert()
  end

  @spec update_source_schema_for_source(TFS.t(), Source.t()) ::
          {:ok, SourceSchema.t()} | {:error, Ecto.Changeset.t()}
  def update_source_schema_for_source(
        attrs,
        %Source{} = source
      ) do
    source_schema = get_source_schema_by(source_id: source.id)

    update_source_schema(source_schema, attrs)
  end

  @spec update_source_schema(SourceSchema.t(), map()) ::
          {:ok, SourceSchema.t()} | {:error, term()}

  def update_source_schema(%SourceSchema{} = source_schema, attrs) do
    source_schema
    |> SourceSchema.changeset(attrs)
    |> RepoWithCache.update()
  end

  @spec delete_source_schema(SourceSchema.t()) :: {:ok, SourceSchema.t()} | {:error, term}
  def delete_source_schema(%SourceSchema{} = source_schema) do
    RepoWithCache.delete(source_schema)
  end

  @spec change_source_schema(SourceSchema.t(), map()) :: Ecto.Changeset.t()
  def change_source_schema(%SourceSchema{} = source_schema, attrs \\ %{}) do
    SourceSchema.changeset(source_schema, attrs)
  end
end
