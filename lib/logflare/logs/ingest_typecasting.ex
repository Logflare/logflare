defmodule Logflare.Logs.IngestTypecasting do
  @moduledoc """
   Casts log event metadata values according to the type casting rules
  """
  import Logflare.EnumDeepUpdate, only: [update_all_values_deep: 2, update_in_enum: 3]
  @transform_directives_key "@logflareTransformDirectives"

  @spec maybe_apply_transform_directives(map()) :: map()
  def maybe_apply_transform_directives(log_params)
      when is_map_key(log_params, @transform_directives_key) do
    {tr_dirs, log_params} = Map.pop(log_params, @transform_directives_key)

    tr_dirs
    |> Enum.reduce(log_params, fn
      {"numbersToFloats", true}, acc ->
        metadata =
          acc
          |> Map.get("metadata")
          |> update_all_values_deep(fn
            n when is_integer(n) -> n * 1.0
            n when is_float(n) -> n
            n -> n
          end)

        %{acc | "metadata" => metadata}

      _, acc ->
        acc
    end)
  end

  def maybe_apply_transform_directives(log_params), do: log_params

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
