defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.SeriesIdentity do
  @moduledoc """
  Preserves the resource and scope identity available in a normalized LogEvent.

  Context IDs hash a typed JSON representation with sorted map entries, ordered
  lists, base64 binaries, and IEEE 754 float bytes. This keeps identities stable
  across map insertion order and runtime upgrades without flattening attributes.
  """

  @scope_fields ~w(name version schema_url)

  @spec labels(map()) :: %{String.t() => String.t()} | :error
  def labels(body) do
    if valid_context?(body["resource"]) and valid_context?(body["scope"]) do
      body
      |> Map.get("scope")
      |> scope_labels()
      |> put_context_id("logflare_resource_id", body["resource"])
      |> put_context_id("logflare_scope_id", body["scope"])
    else
      :error
    end
  end

  @spec valid_context?(term()) :: boolean()
  defp valid_context?(context) when is_map(context), do: valid_value?(context)
  defp valid_context?(_context), do: true

  @spec valid_value?(term()) :: boolean()
  defp valid_value?(value) when is_map(value) do
    value
    |> Map.to_list()
    |> Enum.all?(fn {key, value} -> valid_value?(key) and valid_value?(value) end)
  end

  defp valid_value?(value) when is_list(value), do: valid_list?(value)
  defp valid_value?(value) when is_tuple(value), do: value |> Tuple.to_list() |> valid_list?()

  defp valid_value?(value)
       when is_binary(value) or is_integer(value) or is_float(value) or is_atom(value),
       do: true

  defp valid_value?(_value), do: false

  @spec valid_list?(term()) :: boolean()
  defp valid_list?([]), do: true
  defp valid_list?([head | tail]), do: valid_value?(head) and valid_list?(tail)
  defp valid_list?(_value), do: false

  @spec scope_labels(term()) :: map()
  defp scope_labels(scope) when is_map(scope) do
    for field <- @scope_fields,
        value = Map.get(scope, field),
        is_binary(value) and value != "",
        into: %{},
        do: {"otel_scope_" <> field, value}
  end

  defp scope_labels(_scope), do: %{}

  @spec put_context_id(map(), String.t(), term()) :: map()
  defp put_context_id(labels, name, context) when is_map(context) and map_size(context) > 0 do
    encoded = context |> canonical() |> Jason.encode!()
    id = :sha256 |> :crypto.hash(encoded) |> Base.encode16(case: :lower)
    Map.put(labels, name, id)
  end

  defp put_context_id(labels, _name, _context), do: labels

  @spec canonical(term()) :: list()
  defp canonical(value) when is_map(value) do
    entries =
      value
      |> Map.to_list()
      |> Enum.map(fn {key, value} -> [canonical(key), canonical(value)] end)
      |> Enum.sort()

    ["map", entries]
  end

  defp canonical(value) when is_list(value), do: ["list", Enum.map(value, &canonical/1)]

  defp canonical(value) when is_tuple(value),
    do: ["tuple", value |> Tuple.to_list() |> Enum.map(&canonical/1)]

  defp canonical(value) when is_binary(value), do: ["binary", Base.encode64(value)]
  defp canonical(value) when is_integer(value), do: ["integer", Integer.to_string(value)]
  defp canonical(value) when is_float(value), do: ["float", Base.encode16(<<value::float-64>>)]
  defp canonical(value) when is_atom(value), do: ["atom", Atom.to_string(value)]
end
