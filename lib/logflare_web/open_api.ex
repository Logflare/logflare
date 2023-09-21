defmodule LogflareWeb.OpenApi do
  alias OpenApiSpex.Schema

  defmacro __using__(properties: properties, required: required) do
    quote do
      require OpenApiSpex

      OpenApiSpex.schema(%{
        type: :object,
        properties: unquote(properties),
        required: unquote(required)
      })

      def response() do
        {"#{__MODULE__.schema().title} Response", "application/json", __MODULE__}
      end

      def params() do
        {"#{__MODULE__.schema().title} Parameters", "application/json", __MODULE__}
      end
    end
  end

  defmodule List do
    require OpenApiSpex

    OpenApiSpex.schema(%{type: :array, items: Endpoint})

    def response(module) do
      {
        "#{module.schema.title} List Response",
        "application/json",
        %Schema{type: :array, items: module}
      }
    end
  end

  defmodule Created do
    def response(module), do: {"Created Response", "application/json", module}
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

  defmodule ServerError do
    require OpenApiSpex
    OpenApiSpex.schema(%{})

    def response(), do: {"Server error", "text/plain", __MODULE__}
  end
end
