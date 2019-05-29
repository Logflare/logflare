defmodule Logflare.Validator.DeepFieldTypes do
  @moduledoc """
  Validates that types of values for the same field path are the same
  """
  @spec valid?(map()) :: boolean()
  def valid?(map) when is_map(map) do
    try do
      map
      |> Iteraptor.map(fn {k, v} -> {k, type_of(v)} end)
      |> deep_merge_enums()
      |> is_map
    rescue
      _e in RuntimeError ->
        false
    end
  end

  @spec deep_merge_enums(list(map) | map) :: map
  defp deep_merge_enums(map) when is_map(map) do
    for {k, v} <- map, into: Map.new() do
      v = if is_list(v), do: deep_merge_enums(v), else: v

      {k, v}
    end
  end

  defp deep_merge_enums(maps) do
    resolver = fn
      _, original, override when is_list(original) and is_list(override) ->
        deep_merge_enums(original ++ override)

      _, original, override when is_atom(original) or is_atom(override) ->
        if original != override do
          raise("typeerror")
        else
          original
        end

      _, _original, _override ->
        DeepMerge.continue_deep_merge()
    end

    Enum.reduce(maps, %{}, fn map, acc ->
      DeepMerge.deep_merge(acc, map, resolver)
    end)
  end

  defp type_of(arg) when is_binary(arg), do: :binary
  defp type_of(arg) when is_map(arg), do: :map
  defp type_of(arg) when is_list(arg), do: :list
  defp type_of(arg) when is_bitstring(arg), do: :bitstring
  defp type_of(arg) when is_float(arg), do: :float
  defp type_of(arg) when is_function(arg), do: :function
  defp type_of(arg) when is_integer(arg), do: :integer
  defp type_of(arg) when is_pid(arg), do: :pid
  defp type_of(arg) when is_port(arg), do: :port
  defp type_of(arg) when is_reference(arg), do: :reference
  defp type_of(arg) when is_tuple(arg), do: :tuple
  defp type_of(arg) when is_atom(arg), do: :atom
end
