defmodule Logflare.Sources.SourceSchema do
  use Logflare.Commons
  use TypedEctoSchema
  import Ecto.Changeset

  use Logflare.Changefeeds.ChangefeedSchema, derive_virtual: [:type_map, :field_count]
  @min_number_of_schema_fields 3

  typed_schema "source_schemas" do
    field :bigquery_schema, Ecto.Term
    field :field_count, :integer, default: 0, virtual: true
    field :type_map, Ecto.Term, default: %{}, virtual: true

    belongs_to :source, Logflare.Source

    timestamps()
  end

  @doc false
  def changeset(source_schema, attrs) do
    source_schema
    |> cast(attrs, [:bigquery_schema])
    |> validate_required([:bigquery_schema])
    |> foreign_key_constraint(:source_id)
    |> unique_constraint(:source_id, name: "source_schemas_source_id_index")
  end

  def derived_validations(%Ecto.Changeset{} = changeset) do
    changeset
    |> validate_number(:field_count, greater_than_or_equal_to: @min_number_of_schema_fields)
  end

  def derive(:type_map, struct, _virtual_struct) do
    Logflare.Google.BigQuery.SchemaUtils.to_typemap(struct.bigquery_schema)
  end

  def derive(:field_count, _struct, virtual_struct) do
    virtual_struct.type_map
    |> Iteraptor.to_flatmap()
    |> Enum.count()
  end
end
