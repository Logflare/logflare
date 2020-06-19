defmodule Logflare.Logs.IngestTypecasting do
  @moduledoc """
   Casts log event metadata values according to the type casting rules
  """
  import Logflare.EnumDeepUpdate, only: [update_in_enum: 3]

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
          update_in_enum(acc, keys, &String.to_float/1)

        _, acc ->
          acc
      end
    )
  end
end
