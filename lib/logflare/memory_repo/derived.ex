defmodule Logflare.EctoDerived do
  alias Logflare.EctoSchemaReflection
  alias Logflare.MemoryRepo

  def merge_virtual(nil), do: nil

  def merge_virtual(results) when is_list(results) do
    Enum.map(results, &merge_virtual/1)
  end

  def merge_virtual(result) do
    %mod{} = result

    virtual_schema = Module.concat(mod, Virtual)

    if Code.ensure_loaded?(virtual_schema) do
      virtual =
        virtual_schema
        |> MemoryRepo.get(result.id)
        |> Map.take(EctoSchemaReflection.virtual_fields(mod))

      Map.merge(result, virtual)
    else
      result
    end
  end
end
