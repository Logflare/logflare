defmodule Logflare.Logs.Ingest.MetadataCleaner do
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
        cleaned = deep_reject_nil_and_empty(x)
        if nil_or_empty?(cleaned), do: acc, else: [cleaned | acc]

      x, acc ->
        [x | acc]
    end)
    |> Enum.reverse()
  end

  @spec deep_reject_nil_and_empty(map) :: map
  def deep_reject_nil_and_empty(map) when is_map(map) do
    Enum.reduce(map, %{}, fn
      {_, v}, acc when nil_or_empty(v) ->
        acc

      {k, v}, acc when is_map(v) or is_list(v) ->
        cleaned = deep_reject_nil_and_empty(v)
        if nil_or_empty?(cleaned), do: acc, else: Map.put(acc, k, cleaned)

      {k, v}, acc ->
        Map.put(acc, k, v)
    end)
  end

  def nil_or_empty?(x) when nil_or_empty(x), do: true
  def nil_or_empty?(_), do: false
end
