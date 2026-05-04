defmodule Logflare.GenDecorators do
  @moduledoc """
  A collection of generic decorator functions for Decorators
  """
  use Decorator.Define, pass_through_on_error_field: 0

  def pass_through_on_error_field(body, %{args: [map]}) do
    quote do
      if is_nil(unquote(map).error) do
        unquote(body)
      else
        unquote(map)
      end
    end
  end
end
