defmodule LogflareWeb.SourceBqSchemaComponent do
  use LogflareWeb, :live_component
  use Logflare.Commons
  alias LogflareWeb.Helpers.BqSchema

  @impl true
  def update(assigns, socket) do
    source_schema = SourceSchemas.get_source_schema_by(source_id: assigns.source.id)
    {:ok, assign(socket, %{source_schema: source_schema})}
  end

  @impl true
  def render(%{source_schema: source_schema}) do
    BqSchema.format_bq_schema(source_schema.bigquery_schema)
  end
end
