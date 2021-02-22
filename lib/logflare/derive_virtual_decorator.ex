defmodule Logflare.DeriveVirtualDecorator do
  use Decorator.Define, update_virtual_fields: 0, update_virtual_fields: 1

  import Logflare.LocalRepo.EctoDerived, only: [merge_virtual: 1]

  def update_virtual_fields(:preload, body, %{args: args} = context) do
    quote do
      result = unquote(body)

      if is_struct(result) do
        [_, assoc | _] = unquote(args)

        new_assoc =
          case assoc do
            [{k, %Ecto.Query{} = v} | _] = kw when is_list(kw) and is_atom(k) and is_struct(v) ->
              kw

            x when is_atom(x) ->
              result
              |> Map.get(assoc)
              |> merge_virtual()
          end

        %{result | assoc => new_assoc}
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
