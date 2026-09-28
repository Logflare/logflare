defmodule LogflareWeb.SourceBqSchemaComponent do
  @moduledoc false
  use LogflareWeb, :live_component
  alias Logflare.SourceSchemas
  alias LogflareWeb.Helpers.BqSchema

  @impl true
  def render(%{source: source} = assigns) do
    schema_flatmap = SourceSchemas.source_schema_flatmap_or_default(source)

    assigns = assign(assigns, :schema, BqSchema.format_schema(schema_flatmap))

    ~H"""
    <div>
      {@schema}
    </div>
    """
  end
end
