defmodule LogflareWeb.OpenApiSchemas do
  alias OpenApiSpex.Schema

  defmodule Endpoint do
    require OpenApiSpex

    OpenApiSpex.schema(%{
      type: :object,
      properties: %{
        token: %Schema{type: :string},
        name: %Schema{type: :string},
        query: %Schema{type: :string},
        source_mapping: %Schema{type: :object},
        sandboxable: %Schema{type: :boolean},
        cache_duration_seconds: %Schema{type: :integer},
        proactive_requerying_seconds: %Schema{type: :integer},
        max_limit: %Schema{type: :integer},
        enable_auth: %Schema{type: :boolean},
        inserted_at: %Schema{
          type: :string,
          description: "Creation timestamp",
          format: :"date-time"
        },
        updated_at: %Schema{type: :string, description: "Update timestamp", format: :"date-time"}
      },
      required: [:name, :query]
    })

    def response(), do: {"Endpoint Response", "application/json", __MODULE__}
  end

  defmodule EndpointList do
    require OpenApiSpex

    OpenApiSpex.schema(%{type: :array, items: Endpoint})
    def response(), do: {"Endpoint List Response", "application/json", __MODULE__}
  end

  defmodule EndpointCreate do
    def params(), do: {"Endpoint Create Params", "application/json", Endpoint}
  end

  defmodule Created do
    def response(schema), do: {"Created Response", "application/json", schema}
  end

  defmodule Accepted do
    require OpenApiSpex
    OpenApiSpex.schema(%{})

    def response(), do: {"Accepted Response", "text/plain", __MODULE__}
  end

  defmodule NotFound do
    require OpenApiSpex
    OpenApiSpex.schema(%{})

    def response(), do: {"Not found", "text/plain", __MODULE__}
  end
end
