defmodule Logflare.Logs.IngestTypecasting do
  @moduledoc """
   Casts log event metadata values according to the type casting rules
  """

  def maybe_cast_batch(batch, %{"schemaTypecasts" => typecasts}) do
    Enum.map(batch, &cast(&1, typecasts))
  end

  def maybe_cast_batch(batch, _), do: batch

  @doc false
  def cast(data, typecasts) do
    Enum.reduce(
      typecasts,
      data,
      fn
        %{path: keys, from: "string", to: "float"}, acc ->
          update(acc, keys, &String.to_float/1)
      end
    )
  end

  defp update(data, [key], fun) when is_map(data) do
    if data[key] do
      Map.update!(data, key, fun)
    else
      data
    end
  end

  defp update(data, [key | keys_rest], fun) when is_map(data) do
    if data[key] do
      Map.update!(data, key, &update(&1, keys_rest, fun))
    else
      data
    end
  end

  defp update(data, keys, fun) when is_list(data) do
    Enum.map(data, &update(&1, keys, fun))
  end
end
