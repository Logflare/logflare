defmodule Logflare.Logs.Ingest.MetadataCleaner do
  @moduledoc """
  Deeply rejects nils and empty containers in nested enumerables.
  Also provides flattening of nested maps into dot-delimited key paths.
  """

  import Logflare.Utils.Guards, only: [is_non_empty_map: 1]

  @type flat_key :: String.t()
  @typep pair_acc :: [{flat_key(), term()}]

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

  @doc """
  Flattens a map into a single-level map with dot-delimited string keys.
  Lists use integer indices as key segments (e.g., `"tags.0"`, `"tags.1"`).

  Does not remove nils or empty containers — the input is expected to be
  already cleaned.
  """
  @spec flatten(map()) :: %{flat_key() => term()}
  def flatten(map) when is_map(map) do
    map
    |> do_flatten_map()
    |> :lists.reverse()
    |> :maps.from_list()
  end

  def nil_or_empty?(x) when nil_or_empty(x), do: true
  def nil_or_empty?(_), do: false

  @spec do_flatten_map(map(), [String.t()], pair_acc()) :: pair_acc()
  defp do_flatten_map(map, prefix \\ [], acc \\ []) do
    do_flatten_pairs(:maps.to_list(map), prefix, acc)
  end

  @spec do_flatten_pairs([{term(), term()}], [String.t()], pair_acc()) :: pair_acc()
  defp do_flatten_pairs([], _prefix, acc), do: acc

  defp do_flatten_pairs([{k, v} | rest], prefix, acc) when is_non_empty_map(v) do
    acc = do_flatten_map(v, [k | prefix], acc)
    do_flatten_pairs(rest, prefix, acc)
  end

  defp do_flatten_pairs([{k, [_ | _] = v} | rest], prefix, acc) do
    acc = do_flatten_list(v, 0, [k | prefix], acc)
    do_flatten_pairs(rest, prefix, acc)
  end

  defp do_flatten_pairs([{k, v} | rest], prefix, acc) do
    do_flatten_pairs(rest, prefix, [{build_key([k | prefix]), v} | acc])
  end

  @spec do_flatten_list(list(), non_neg_integer(), [String.t()], pair_acc()) :: pair_acc()
  defp do_flatten_list([], _idx, _prefix, acc), do: acc

  defp do_flatten_list([v | rest], idx, prefix, acc) when is_non_empty_map(v) do
    acc = do_flatten_map(v, [Integer.to_string(idx) | prefix], acc)
    do_flatten_list(rest, idx + 1, prefix, acc)
  end

  defp do_flatten_list([[_ | _] = v | rest], idx, prefix, acc) do
    acc = do_flatten_list(v, 0, [Integer.to_string(idx) | prefix], acc)
    do_flatten_list(rest, idx + 1, prefix, acc)
  end

  defp do_flatten_list([v | rest], idx, prefix, acc) do
    do_flatten_list(rest, idx + 1, prefix, [
      {build_key([Integer.to_string(idx) | prefix]), v} | acc
    ])
  end

  @spec build_key([term()]) :: flat_key()
  defp build_key([single]) when is_binary(single), do: single
  defp build_key([single]), do: to_string(single)

  defp build_key(reversed_parts) do
    reversed_parts
    |> Enum.reverse()
    |> Enum.join(".")
  end
end
