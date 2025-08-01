defmodule LogflareWeb.SourceBqSchemaComponent do
  @moduledoc false
  use LogflareWeb, :live_component
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.SourceSchemas
  alias Logflare.Source.BigQuery.SchemaBuilder

  @impl true
  def render(%{source: source}) do
    bq_schema =
      SourceSchemas.Cache.get_source_schema_by(source_id: source.id)
      |> case do
        nil -> SchemaBuilder.initial_table_schema()
        %_{bigquery_schema: schema} -> schema
      end

    BqSchema.format_bq_schema(bq_schema)
  end
end
