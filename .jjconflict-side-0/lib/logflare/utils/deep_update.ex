defmodule Logflare.EnumDeepUpdate do
  @moduledoc false
  @type data :: map() | list(map())
  @doc """
  Performs a nested value update on an enum

    iex> update_in_enum(%{some: %{nested: "value"}}, [:some, :nested], fn v -> v <> "_new" end)
    %{some: %{nested: "value_new"}}

  Also works for a list of maps
    iex> update_in_enum([%{some: "value"}], [:some], fn v -> v <> "_new" end)
    [%{some: "value_new"}]

  TODO: should work for keyword list as well
  """
  @typep keys :: [atom() | String.t()]
  @spec update_in_enum(data(), keys(), fun()) :: map() | list(map())
  def update_in_enum(data, [key], fun) when is_map(data) do
    if data[key] do
      Map.update!(data, key, fun)
    else
      data
    end
  end

  def update_in_enum(data, [key | keys_rest], fun) when is_map(data) do
    if data[key] do
      Map.update!(data, key, &update_in_enum(&1, keys_rest, fun))
    else
      data
    end
  end

  def update_in_enum(data, keys, fun) when is_list(data) do
    Enum.map(data, &update_in_enum(&1, keys, fun))
  end

  @doc """
  Performs a deep map update on keys. Only works for string keys

    iex> data = %{"other"=>  nil, "some"=> %{"nested"=> nil}}
    iex> update_all_keys_deep(data, fn v -> v <> "_new" end)
    %{"other_new"=> nil, "some_new"=> %{"nested_new"=> nil}}

  Also works for nested list of maps

    iex> data = %{"some"=> [%{"nested" => nil}]}
    iex> update_all_keys_deep(data, fn v -> v <> "_new" end)
    %{"some_new"=> [%{"nested_new"=> nil}]}
  """
  @typep string_map :: %{String.t() => term()}
  @typep string_map_data :: string_map() | [string_map()]
  @spec update_all_keys_deep(string_map_data(), fun()) :: string_map_data()
  def update_all_keys_deep(data, fun) when is_map(data) do
    data
    |> Enum.map(fn
      {k, v} when is_map(v) or is_list(v) ->
        {fun.(k), update_all_keys_deep(v, fun)}

      {k, v} ->
        {fun.(k), v}
    end)
    |> Map.new()
  end

  def update_all_keys_deep(data, fun) when is_list(data) do
    Enum.map(data, &update_all_keys_deep(&1, fun))
  end

  def update_all_keys_deep(data, _), do: data

  def update_all_values_deep(data, fun) when is_list(data) do
    Enum.map(data, &update_all_values_deep(&1, fun))
  end

  def update_all_values_deep(data, fun) when is_map(data) do
    data
    |> Enum.map(fn
      {k, v} when is_map(v) or is_list(v) ->
        {k, update_all_values_deep(v, fun)}

      {k, v} ->
        {k, fun.(v)}
    end)
    |> Map.new()
  end

  def update_all_values_deep(data, fun) do
    fun.(data)
  end
end
