defmodule Logflare.Sources.SourceSchema do
  use Logflare.Commons
  use Ecto.Schema
  import Ecto.Changeset
  use Logflare.ChangefeedSchema

  schema "source_schemas" do
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

  def compute_virtual_fields(source_schema) do
    fun = fn field ->
      case field do
        :type_map ->
          &Logflare.Google.BigQuery.SchemaUtils.to_typemap(&1.bigquery_schema)

        :field_count ->
          &(&1.type_map
            |> Iteraptor.to_flatmap()
            |> Enum.count())

        _ ->
          & &1
      end
    end

    virtual_schema = Module.concat(__MODULE__, Virtual)

    for field <- EctoSchemaReflection.virtual_fields(__MODULE__),
        reduce: struct(virtual_schema) do
      acc ->
        compute_field = fun.(field)
        %{acc | field => compute_field.(source_schema)}
    end
  end
end
