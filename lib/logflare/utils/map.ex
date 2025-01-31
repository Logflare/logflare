defmodule Logflare.Utils.Map do
  @doc """
  Retrieves a key, regardless of whether it is a string map or an atom map

    iex> #{__MODULE__}.get(%{test: 123}, :test)
    123
    iex> #{__MODULE__}.get(%{"test"=> 123}, :test)
    123
  """
  @spec get(map :: map(), key :: atom()) :: term() | nil
  def get(map, key) when is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end
end
