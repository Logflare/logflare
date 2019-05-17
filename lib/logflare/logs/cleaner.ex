defmodule Logflare.Logs.Injest.MetadataCleaner do
  @moduledoc """
  Deeply rejects nils and empty containers in nested enumerables
  """
  defguardp nil_or_empty(x) when x in [%{}, [], "", {}, nil]

  @spec deep_reject_nil_and_empty(list(term)) :: list(term)
  def deep_reject_nil_and_empty(xs) when is_list(xs) do
    xs
    |> Enum.reduce([], fn
      x, acc when nil_or_empty(x) ->
        acc

      x, acc when is_map(x) or is_list(x) ->
        [deep_reject_nil_and_empty(x) | acc]

      x, acc ->
        [x | acc]
    end)
    |> Enum.reverse()
  end

  @spec deep_reject_nil_and_empty(map) :: list(map)
  def deep_reject_nil_and_empty(map) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn
      {_, v}, acc when nil_or_empty(v) ->
        acc

      {k, v}, acc when is_map(v) or is_list(v) ->
        Map.put(acc, k, deep_reject_nil_and_empty(v))

      {k, v}, acc ->
        Map.put(acc, k, v)
    end)
    |> Map.new()
  end
end
