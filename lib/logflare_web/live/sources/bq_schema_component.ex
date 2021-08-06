defmodule LogflareWeb.SourceBqSchemaComponent do
  use LogflareWeb, :live_component
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.SourceSchemas

  @impl true
  def render(%{source: source}) do
    bq_schema =
      SourceSchemas.get_source_schema_by(source_id: source.id)
      |> Map.get(:bigquery_schema)

    BqSchema.format_bq_schema(bq_schema)
  end
end
