defmodule LogflareWeb.Sources.BqSchemaLive do
  use LogflareWeb, :live_component
  alias LogflareWeb.Helpers.BqSchema
  alias Logflare.Sources

  @impl true
  def render(%{source: source}) do
    BqSchema.format_bq_schema(source.source_schema.bigquery_schema)
  end
end
