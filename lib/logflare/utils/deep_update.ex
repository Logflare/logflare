defmodule Logflare.EnumDeepUpdate do
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
