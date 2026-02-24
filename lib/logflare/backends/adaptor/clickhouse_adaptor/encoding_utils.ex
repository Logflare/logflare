defmodule Logflare.Backends.Adaptor.ClickHouseAdaptor.EncodingUtils do
  @moduledoc false

  @spec sanitize_for_json(term()) :: Jason.Encoder.t()
  def sanitize_for_json(value) when is_map(value) do
    Map.new(value, fn {k, v} -> {k, sanitize_for_json(v)} end)
  end

  def sanitize_for_json(value) when is_list(value) do
    Enum.map(value, &sanitize_for_json/1)
  end

  def sanitize_for_json(value) when is_tuple(value) do
    value |> Tuple.to_list() |> Enum.map(&sanitize_for_json/1)
  end

  def sanitize_for_json(value)
      when is_port(value) or is_pid(value) or is_reference(value) or is_function(value) do
    inspect(value)
  end

  def sanitize_for_json(value), do: value
end
