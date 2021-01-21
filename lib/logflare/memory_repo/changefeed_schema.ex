defmodule Logflare.ChangefeedSchema do
  alias Logflare.EctoSchemaReflection
  @callback changefeed_changeset(map()) :: Ecto.Changeset.t()

  defmacro __using__(opts) do
    derive_virtual = Keyword.get(opts, :derive_virtual)

    quote do
      @before_compile Logflare.ChangefeedSchema
      @after_compile Logflare.ChangefeedSchema

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
      derive_virtual = Keyword.get(env.module.__info__(:attributes), :derive_virtual)
      create_schema_virtual(env.module, derive_virtual)
    end
  end

  def create_schema_virtual(schema, derive_virtual) do
    module_name = Module.concat(schema, Virtual)
    table_name = "#{schema.__schema__(:source)}_virtual"

    contents =
      quote do
        use TypedEctoSchema
        import Ecto.Changeset

        @primary_key false
        typed_schema unquote(table_name) do
          Ecto.Schema.field(:id, :integer, primary_key: true)

          for f <- EctoSchemaReflection.virtual_fields(unquote(schema)) do
            type = EctoSchemaReflection.virtual_field_type(unquote(schema), f)
            Ecto.Schema.field(f, type, [])
          end
        end

        def changefeed_changeset(non_virtual_struct) do
          %schema{} = non_virtual_struct

          params =
            for field <- unquote(derive_virtual), reduce: %{} do
              virtual_params ->
                Map.put(
                  virtual_params,
                  field,
                  schema.derive(field, non_virtual_struct, virtual_params)
                )
            end

          changeset =
            struct(unquote(module_name), id: non_virtual_struct.id)
            |> cast(params, EctoSchemaReflection.fields(unquote(module_name)))
            |> validate_required([:id] ++ unquote(derive_virtual))

          if Keyword.get(unquote(schema).__info__(:functions), :derived_validations) do
            changeset
            |> unquote(schema).derived_validations()
          else
            changeset
          end
        end
      end

    Module.create(module_name, contents, Macro.Env.location(__ENV__))
    :ok
  end
end
