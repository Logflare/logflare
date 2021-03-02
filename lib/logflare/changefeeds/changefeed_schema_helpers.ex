defmodule Logflare.Changfeeds.ChangefeedSchema.Helpers do
  alias Logflare.EctoSchemaReflection

  def changefeed_changeset(non_virtual_struct, module_name, derived_virtual) do
    %schema{} = non_virtual_struct

    params =
      for field <- derived_virtual, reduce: %{} do
        virtual_params ->
          Map.put(
            virtual_params,
            field,
            schema.derive(field, non_virtual_struct, virtual_params)
          )
      end

    changeset =
      module_name
      |> struct(id: non_virtual_struct.id)
      |> Ecto.Changeset.cast(params, EctoSchemaReflection.fields(module_name))
      |> Ecto.Changeset.validate_required([:id] ++ derived_virtual)

    if Keyword.get(schema.__info__(:functions), :derived_validations) do
      schema.derived_validations(changeset)
    else
      changeset
    end
  end
end
