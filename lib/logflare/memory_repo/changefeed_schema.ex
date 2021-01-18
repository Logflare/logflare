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

      quote do
        def changefeed_changeset(attrs) do
          Logflare.EctoChangesetExtras.cast_all_fields(
            struct(__MODULE__),
            attrs
          )
        end

        defoverridable changefeed_changeset: 1

        @behaviour Logflare.ChangefeedSchema
      end
    end
  end
end
