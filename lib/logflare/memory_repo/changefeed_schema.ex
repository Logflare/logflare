defmodule Logflare.ChangefeedSchema do
  alias Logflare.EctoSchemaReflection
  @callback changefeed_changeset(map()) :: Ecto.Changeset.t()

  defmacro __using__(_opts) do
    quote do
      @before_compile Logflare.ChangefeedSchema
      @after_compile Logflare.ChangefeedSchema
    end
  end

  defmacro __before_compile__(env) do
    quote do
      @module_defines_changefeed_changeset Module.defines?(
                                             unquote(env.module),
                                             {:changefeed_changeset, 2}
                                           )

      unless @module_defines_changefeed_changeset do
        def changefeed_changeset(struct \\ struct(__MODULE__), attrs)

        def changefeed_changeset(struct, attrs) do
          Logflare.EctoChangesetExtras.cast_all_fields(
            struct,
            attrs
          )
        end

        defoverridable changefeed_changeset: 2
      end

      @behaviour Logflare.ChangefeedSchema
    end
  end

  defmacro __after_compile__(env, _bytecode) do
    quote do
      if not @module_defines_changefeed_changeset and
           not Enum.empty?(EctoSchemaReflection.embeds(unquote(env.module))) do
        message = """
        default implementation of changefeed_changeset/2 injected by ChangefeedSchema doesn't handle schema embeds \
        (in module #{inspect(unquote(env.module))}).

        You'll need to implement the changefeed_changeset/2 function for module #{
          inspect(unquote(env.module))
        }.
        """

        IO.warn(message, Macro.Env.stacktrace(unquote(env)))

        throw("Implement changefeed_changeset/2 for module #{inspect(unquote(env.module))}")
      end
    end

    if not Enum.empty?(EctoSchemaReflection.virtual_fields(env.module)) do
      create_schema_virtual(env.module)
    end
  end

  def create_schema_virtual(schema) do
    module_name = Module.concat(schema, Virtual)
    table_name = "#{schema.__schema__(:source)}_virtual"

    contents =
      quote do
        use TypedEctoSchema

        typed_schema unquote(table_name) do
          for f <- EctoSchemaReflection.virtual_fields(unquote(schema)) do
            type = EctoSchemaReflection.virtual_field_type(unquote(schema), f)
            Ecto.Schema.field(f, type, [])
          end
        end
      end

    Module.create(module_name, contents, Macro.Env.location(__ENV__))
    :ok
  end
end
