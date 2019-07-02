defmodule Logflare.EctoQueryBQ.NestedPath do
  def to_map(pathvalues) do
    nested_map = %{}

    Enum.reduce(pathvalues, nested_map, fn %{path: path, value: value}, nested_map ->
      # If all atoms do not exist, Iteraptor fails
      path |> String.split(".") |> Enum.each(&String.to_atom/1)
      path = String.to_atom(path)
      mappathvalue = Iteraptor.from_flatmap(%{path => value})
      DeepMerge.deep_merge(nested_map, mappathvalue)
    end)
  end
end
