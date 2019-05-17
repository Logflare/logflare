defmodule Logflare.Logs.Injest.MetadataCleaner do
  def reject_empty_kvs(xs) when is_list(xs) do
    xs
    |> Enum.reduce([], fn
      x, acc when is_map(x) or is_list(x) ->
        [reject_empty_kvs(x) | acc]

      nil, acc ->
        acc

      x, acc ->
        [x | acc]
    end)
    |> Enum.reverse()
  end

  def reject_empty_kvs(map) when is_map(map) do
    map
    |> Enum.reduce(%{}, fn
      {_, nil}, acc ->
        acc

      {k, v}, acc when is_map(v) or is_list(v) ->
        Map.put(acc, k, reject_empty_kvs(v))

      {k, v}, acc ->
        Map.put(acc, k, v)
    end)
    |> Map.new()
  end
end
