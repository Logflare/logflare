defmodule Logflare.ChangefeedSchema do
  alias Logflare.EctoSchemaReflection
  @callback changefeed_changeset(map()) :: Ecto.Changeset.t()

  defmacro __using__(_opts) do
    quote do
      @after_compile Logflare.ChangefeedSchema
    end
  end

  defmacro __after_compile__(env, _bytecode) do
    if not Module.defines?(env.module, {:changefeed_changeset, 1}) and
         not Enum.empty?(EctoSchemaReflection.embeds(env.module)) do
      message = """
      default implementation of changefeed_changeset/1 injected by ChangefeedSchema doesn't handle schema embeds \
      (in module #{inspect(env.module)}).

      You'll need to implement the changefeed_changeset/1 function for module #{
        inspect(env.module)
      }.
      """

      IO.warn(message, Macro.Env.stacktrace(env))

      throw("Implement changefeed_changeset/1 for module #{inspect(env.module)}")

    module_name =
      Module.concat(
        env.module,
        Virtual
      )

    if not Enum.empty?(EctoSchemaReflection.virtual_fields(env.module)) do
      Macro.escape(
        defmodule quote do: unquote(module_name) do
          use TypedEctoSchema

          typed_schema "#{env.module.__schema__(:source)}_virtual" do
            for f <- EctoSchemaReflection.virtual_fields(env.module) do
              type = EctoSchemaReflection.virtual_field_type(env.module, f)

              quote do
                field unquote(f), unquote(type)
              end
            end
          end
        end
      )
    end
  end
end
