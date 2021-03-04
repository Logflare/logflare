defmodule Logflare.Changefeeds.ChangefeedSchema do
  alias Logflare.EctoSchemaReflection
  alias Logflare.LocalRepo.EctoDerived
  @callback changefeed_changeset(map()) :: Ecto.Changeset.t()

  defmacro __using__(opts) do
    derive_virtual = Keyword.get(opts, :derive_virtual)

    quote do
      @before_compile Logflare.Changefeeds.ChangefeedSchema
      @after_compile Logflare.Changefeeds.ChangefeedSchema

      if unquote(derive_virtual) do
        Module.register_attribute(__MODULE__, :derive_virtual, persist: true)
        Module.put_attribute(__MODULE__, :derive_virtual, unquote(derive_virtual))
      end
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

      @behaviour Logflare.Changefeeds.ChangefeedSchema
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
      derive_virtual = Keyword.get(env.module.__info__(:attributes), :derive_virtual)
      create_schema_virtual(env.module, derive_virtual)
    end
  end

  def create_schema_virtual(schema, derive_virtual) do
    module_name = EctoDerived.to_derived_module_name(schema)
    table_name = "#{schema.__schema__(:source)}_virtual"

    contents =
      quote do
        alias Logflare.LocalRepo.EctoDerived
        use TypedEctoSchema
        import Ecto.Changeset

        id_type = unquote(schema).__changeset__.id
        @primary_key {:id, id_type, autogenerate: false}
        typed_schema unquote(table_name) do
          for f <- EctoSchemaReflection.virtual_fields(unquote(schema)) do
            type = EctoSchemaReflection.virtual_field_type(unquote(schema), f)
            Ecto.Schema.field(f, type, [])
          end
        end

        def changefeed_changeset(non_virtual_struct) do
          Helpers.changefeed_changeset(
            non_virtual_struct,
            unquote(module_name),
            unquote(derive_virtual)
          )
        end
      end

    Module.create(module_name, contents, Macro.Env.location(__ENV__))
    :ok
  end
end
