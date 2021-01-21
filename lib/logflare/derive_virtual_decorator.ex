defmodule Logflare.DeriveVirtualDecorator do
  use Decorator.Define, update_virtual_fields: 0, update_virtual_fields: 1

  import Logflare.EctoDerived, only: [merge_virtual: 1]

  def update_virtual_fields(:preload, body, %{args: args} = context) do
    quote do
      result = unquote(body)

      if is_struct(result) do
        [_, assoc | _] = unquote(args)

        %{result | assoc => merge_virtual(Map.get(result, assoc))}
      else
        result
      end
    end
  end

  def update_virtual_fields(body, %{args: args} = context) do
    quote do
      result = unquote(body)

      if is_struct(result) do
        merge_virtual(result)
      else
        result
      end
    end
  end
end
