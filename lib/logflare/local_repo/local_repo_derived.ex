defmodule Logflare.LocalRepo.EctoDerived do
  alias Logflare.EctoSchemaReflection
  alias Logflare.LocalRepo

  @spec merge_virtual(nil | [struct] | struct) :: nil | [struct] | struct
  def merge_virtual(nil), do: nil

  def merge_virtual(results) when is_list(results) do
    Enum.map(results, &merge_virtual/1)
  end

  def merge_virtual(result) do
    %mod{} = result

    virtual_schema = Module.concat(mod, Virtual)

    if Code.ensure_loaded?(virtual_schema) do
      virtual = LocalRepo.get(virtual_schema, result.id) || %{}
      virtual = Map.take(virtual, EctoSchemaReflection.virtual_fields(mod))

      Map.merge(result, virtual)
    else
      result
    end
  end

  @spec to_derived_module_name(schema :: module()) :: module()
  def to_derived_module_name(schema) when is_atom(schema) do
    Module.concat(schema, Virtual)
  end
end
