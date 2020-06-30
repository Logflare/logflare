defmodule Logflare.Logs.IngestTypecasting do
  @moduledoc """
   Casts log event metadata values according to the type casting rules
  """
  import Logflare.EnumDeepUpdate, only: [update_in_enum: 3]

  def maybe_cast_batch(batch) do
    Enum.map(batch, &maybe_cast_log_params/1)
  end

  def maybe_cast_log_params(%{"body" => body, "typecasts" => typecasts}) do
    typecasts =
      for t <- typecasts do
        t
        |> Enum.map(fn {k, v} -> {String.to_existing_atom(k), v} end)
        |> Map.new()
      end

    Enum.reduce(
      typecasts,
      body,
      fn
        %{path: keys, from: "string", to: "float"}, acc ->
          update_in_enum(acc, keys, fn value ->
            {float, ""} = Float.parse(value)
            float
          end)

        _, acc ->
          acc
      end
    )
  end

  def maybe_cast_log_params(log_params), do: log_params
end
