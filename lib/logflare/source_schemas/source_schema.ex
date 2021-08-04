defmodule Logflare.SourceSchemas.SourceSchema do
  use Ecto.Schema
  import Ecto.Changeset

  schema "source_schemas" do
    field :bigquery_schema, Ecto.Term
    field :schema_flat_map, Ecto.Term

    belongs_to :source, Logflare.Source

    timestamps()
  end

  @doc false
  def changeset(source_schema, attrs) do
    source_schema
    |> cast(attrs, [:bigquery_schema, :schema_flat_map])
    |> validate_required([:bigquery_schema, :schema_flat_map])
    |> foreign_key_constraint(:source_id)
    |> unique_constraint(:source_id, name: "source_schemas_source_id_index")
  end
end
