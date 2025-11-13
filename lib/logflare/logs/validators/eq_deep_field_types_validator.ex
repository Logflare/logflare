defmodule Logflare.Logs.Validators.EqDeepFieldTypes do
  alias Logflare.LogEvent, as: LE
  require Logger

  @moduledoc """
  Validates that types of values for the same field path are the same
  """

  # Public
  def validate(%LE{body: body}, _source) do
    if valid?(body) do
      :ok
    else
      {:error, message()}
    end
  rescue
    e ->
      Logger.warning("Unexpected error at #{__MODULE__}: #{Exception.message(e)}")
      {:error, "Log event payload validation error"}
  catch
    :type_error ->
      {:error, message()}

    {:type_error, v} when is_tuple(v) ->
      {:error, tuple_error_message(v)}

    {:type_error, [tup | _]} when is_tuple(tup) ->
      {:error, tuple_error_message(tup)}

    {:type_error, [first | _]} when is_list(first) ->
      {:error, "Nested lists of lists are not allowed"}

    {:type_error, _} ->
      {:error, message()}
  end

  def validate(%{log_event: %{body: _}}, _source) do
    :ok
  end

  @spec valid?(map()) :: boolean()
  def valid?(map) when is_map(map) do
    map
    |> Iteraptor.map(fn
      {_, v} when is_tuple(v) ->
        throw({:type_error, v})

      {k, v} ->
        {k, type_of(v)}
    end)
    |> deep_merge_enums()
    |> deep_validate_lists_are_homogenous()
    |> is_map()
  end

  def message do
    "Validation error: values with the same field path must have the same type."
  end

  def tuple_error_message(v) do
    "Encountered a tuple: '#{inspect(v)}'. Payloads with Elixir tuples are not supported by Logflare API."
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

      [first | _] = v when is_list(first) ->
        throw({:type_error, v})

      v when is_list(v) ->
        cond do
          is_list_of_enums(v) ->
            deep_validate_lists_are_homogenous(v)

          is_homogenous_list(v) ->
            :noop

          not is_homogenous_list(v) ->
            throw({:type_error, v})
        end

      _ ->
        :noop
    end)

    enum
  end

  @spec deep_merge_enums(list(map) | map) :: map
  def deep_merge_enums(map) when is_map(map) do
    for {k, v} <- map, into: Map.new() do
      v = if is_list(v) and is_list_of_maps(v), do: deep_merge_enums(v), else: v

      {k, v}
    end
  end

  def deep_merge_enums(maps) do
    resolver = fn
      _, original, override when is_list(original) and is_list(override) ->
        merged = (original ++ override) |> Enum.uniq()

        cond do
          is_list_of_enums(merged) ->
            deep_merge_enums(merged)

          Enum.empty?(merged) ->
            {:list, :empty}

          is_homogenous_list(merged) ->
            {:list, hd(merged)}

          not is_homogenous_list(merged) ->
            throw(:type_error)
        end

      _, original, override when is_atom(original) or is_atom(override) ->
        if original != override do
          throw(:type_error)
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

  @spec is_list_of_maps(list(any())) :: boolean()
  defp is_list_of_maps(xs) when is_list(xs) do
    Enum.reduce_while(xs, nil, fn
      x, _acc when is_map(x) -> {:cont, true}
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
