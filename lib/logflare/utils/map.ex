defmodule Logflare.Utils.Map do
  @doc """
  Retrieves a key, regardless of whether it is a string map or an atom map

  ## Examples

    iex> get(%{test: 123}, :test)
    123

    iex> get(%{"test"=> 123}, :test)
    123
  """
  @spec get(map :: map(), key :: atom()) :: term() | nil
  def get(map, key) when is_atom(key) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key))
  end

  @doc """
  Checks if a map does not contain nested maps.

  ## Examples

    iex> flat?(%{a: 1, b: "2"})
    true

    iex> flat?(%{a: %{b: 1}})
    false

    iex> flat?(%{a: %{b: %{c: 1}}})
    false

    iex> flat?(%{a: [1, 2, 3]})
    true

    iex> flat?(%{a: {1, 2, 3}})
    true
  """
  def flat?(map) when is_map(map) do
    Enum.all?(map, fn {_key, value} -> not is_map(value) end)
  end
end
