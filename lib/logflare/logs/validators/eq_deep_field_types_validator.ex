defmodule Logflare.Logs.Validators.EqDeepFieldTypes do
  alias Logflare.LogEvent, as: LE

  @moduledoc """
  Validates that types of values for the same field path are the same
  """

  # Public
  def validate(%LE{body: %{metadata: metadata}}) do
    if valid?(metadata) do
      :ok
    else
      {:error, message()}
    end
  end

  def validate(%{log_event: %{body: _}}) do
    :ok
  end

  @spec valid?(map()) :: boolean()
  def valid?(map) when is_map(map) do
    map
    |> Iteraptor.map(fn {k, v} ->
      {k, type_of(v)}
    end)
    |> deep_merge_enums()
    |> deep_validate_lists_are_homogenous()
    |> is_map
  catch
    _e ->
      false
  end

  def message do
    "Metadata validation error: values with the same field path must have the same type."
  end

  # Private

  def deep_validate_lists_are_homogenous(enum) do
    enum
    |> Enum.map(fn
      {_k, v} -> v
      v -> v
    end)
    |> Enum.each(fn
      v when is_map(v) ->
        deep_validate_lists_are_homogenous(v)

      v when is_list(v) ->
        cond do
          is_list_of_enums(v) ->
            deep_validate_lists_are_homogenous(v)

          is_homogenous_list(v) ->
            :noop

          not is_homogenous_list(v) ->
            throw("typeerror")
        end

      _ ->
        :noop
    end)

    enum
  end

  @spec deep_merge_enums(list(map) | map) :: map
  defp deep_merge_enums(map) when is_map(map) do
    for {k, v} <- map, into: Map.new() do
      v = if is_list(v) and is_list_of_enums(v), do: deep_merge_enums(v), else: v

      {k, v}
    end
  end

  defp deep_merge_enums(maps) do
    resolver = fn
      _, original, override when is_list(original) and is_list(override) ->
        merged = original ++ override

        cond do
          is_list_of_enums(merged) ->
            deep_merge_enums(merged)

          merged == [] ->
            {:list, :empty}

          is_homogenous_list(merged) ->
            {:list, hd(merged)}

          not is_homogenous_list(merged) ->
            throw("typeerror")
        end

      _, original, override when is_atom(original) or is_atom(override) ->
        if original != override do
          throw("typeerror")
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

  @spec is_list_of_enums(list(any())) :: boolean()
  defp is_list_of_enums(xs) when is_list(xs) do
    Enum.reduce_while(xs, nil, fn
      x, _acc when is_map(x) when is_list(x) -> {:cont, true}
      _x, _acc -> {:halt, false}
    end)
  end

  @spec is_homogenous_list(list(any())) :: boolean()
  defp is_homogenous_list(xs) when is_list(xs) do
    list_type =
      Enum.reduce_while(xs, true, fn x, acc ->
        if acc == true or acc == x do
          {:cont, x}
        else
          {:halt, false}
        end
      end)

    if list_type do
      true
    else
      false
    end
  end

  defp type_of(arg) when is_binary(arg), do: :binary
  defp type_of(arg) when is_map(arg), do: :map
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
