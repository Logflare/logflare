defmodule Logflare.Logs.Ingest.MetadataCleaner do
  @moduledoc """
  Deeply rejects nils and empty containers in nested enumerables.
  Also provides flattening of nested maps into dot-delimited key paths.
  """

  @type flat_key :: String.t()

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
    Map.new(do_flatten_map(map, []))
  end

  def nil_or_empty?(x) when nil_or_empty(x), do: true
  def nil_or_empty?(_), do: false

  @spec do_flatten_map(map(), [String.t()]) :: [{flat_key(), term()}]
  defp do_flatten_map(map, prefix) do
    Enum.flat_map(map, fn
      {k, v} when is_map(v) and v != %{} ->
        do_flatten_map(v, [k | prefix])

      {k, v} when is_list(v) and v != [] ->
        do_flatten_list(v, [k | prefix])

      {k, v} ->
        [{build_key([k | prefix]), v}]
    end)
  end

  @spec do_flatten_list(list(), [String.t()]) :: [{flat_key(), term()}]
  defp do_flatten_list(list, prefix) do
    list
    |> Enum.with_index()
    |> Enum.flat_map(fn
      {v, idx} when is_map(v) and v != %{} ->
        do_flatten_map(v, [Integer.to_string(idx) | prefix])

      {v, idx} when is_list(v) and v != [] ->
        do_flatten_list(v, [Integer.to_string(idx) | prefix])

      {v, idx} ->
        [{build_key([Integer.to_string(idx) | prefix]), v}]
    end)
  end

  @spec build_key([String.t()]) :: flat_key()
  defp build_key(reversed_parts) do
    reversed_parts
    |> Enum.reverse()
    |> Enum.join(".")
  end
end
