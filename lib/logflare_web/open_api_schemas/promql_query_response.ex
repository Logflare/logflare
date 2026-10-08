defmodule LogflareWeb.OpenApiSchemas.PromQLQueryResponse do
  @moduledoc """
  Native Prometheus query response, including vector, matrix, scalar, and string results.
  """

  require OpenApiSpex

  alias OpenApiSpex.Schema

  OpenApiSpex.schema(%{
    type: :object,
    properties: %{
      status: %Schema{type: :string, enum: ["success", "error"]},
      data: %Schema{
        type: :object,
        properties: %{
          resultType: %Schema{type: :string, enum: ["vector", "matrix", "scalar", "string"]},
          result: %Schema{
            type: :array,
            items: %Schema{},
            description:
              "Native Prometheus values; sample values remain strings, including NaN and infinities."
          }
        },
        required: [:resultType, :result]
      },
      errorType: %Schema{type: :string},
      error: %Schema{type: :string},
      warnings: %Schema{type: :array, items: %Schema{type: :string}},
      infos: %Schema{type: :array, items: %Schema{type: :string}}
    },
    required: [:status]
  })
end
