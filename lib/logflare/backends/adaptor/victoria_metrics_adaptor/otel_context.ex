defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.OtelContext do
  @moduledoc """
  Converts OpenTelemetry resource and instrumentation scope attributes to
  Prometheus labels.
  """

  import Logflare.Utils.Guards, only: [is_non_empty_binary: 1]

  alias Jason.OrderedObject

  @scope_fields ~w(name version schema_url)
  @scope_metadata Enum.map(@scope_fields, &("otel_scope_" <> &1))

  @type labels :: %{String.t() => String.t()}
  @type label_entry :: {String.t(), String.t(), String.t()}

  @spec target_labels(term()) :: labels()
  def target_labels(resource) when is_non_struct_map(resource) and map_size(resource) > 0 do
    name = service_attribute(resource, "service.name", "_service_name")
    namespace = service_attribute(resource, "service.namespace", "_service_namespace")
    instance = service_attribute(resource, "service.instance.id", "_service_instance_id")
    job = if name && namespace, do: namespace <> "/" <> name, else: name
    labels = %{"instance" => instance || ""}

    if job, do: Map.put(labels, "job", job), else: labels
  end

  def target_labels(_resource), do: %{}

  @spec resource_labels(term()) :: labels()
  def resource_labels(resource) do
    resource
    |> attribute_entries("")
    |> merge_entries()
  end

  @spec scope_labels(term(), labels() | [label_entry()]) :: labels()
  def scope_labels(scope, point_labels \\ %{})

  def scope_labels(scope, point_labels) when is_non_struct_map(scope) do
    metadata =
      for field <- @scope_fields,
          value = Map.get(scope, field),
          is_binary(value) and value != "" and String.valid?(value),
          name = "otel_scope_" <> field,
          do: {name, name, value}

    attributes =
      scope
      |> Map.get("attributes")
      |> attribute_entries("otel_scope_")
      |> Enum.reject(fn {_original, name, _value} -> name in @scope_metadata end)

    merge_entries(point_entries(point_labels) ++ metadata ++ attributes)
  end

  def scope_labels(_scope, point_labels), do: point_labels |> point_entries() |> merge_entries()

  @spec point_entries(labels() | [label_entry()]) :: [label_entry()]
  defp point_entries(labels) when is_map(labels),
    do: Enum.map(labels, fn {name, value} -> {name, name, value} end)

  defp point_entries(entries) when is_list(entries), do: entries

  @spec service_attribute(map(), String.t(), String.t()) :: String.t() | nil
  defp service_attribute(resource, key, normalized_key) do
    case Map.get(resource, key) || Map.get(resource, normalized_key) do
      value when is_non_empty_binary(value) ->
        if String.valid?(value), do: value

      _value ->
        nil
    end
  end

  @spec attribute_entries(term(), String.t()) :: [label_entry()]
  defp attribute_entries(attributes, prefix) when is_non_struct_map(attributes) do
    for {key, value} <- attributes,
        {:ok, key} <- [attribute_key(key)],
        {:ok, value} <- [label_value(value)],
        do: {key, sanitize_name(prefix <> key), value}
  end

  defp attribute_entries(_attributes, _prefix), do: []

  @spec attribute_key(term()) :: {:ok, String.t()} | :error
  defp attribute_key(key) when is_non_empty_binary(key), do: {:ok, key}
  defp attribute_key(key) when is_atom(key), do: key |> Atom.to_string() |> attribute_key()
  defp attribute_key(_key), do: :error

  @spec merge_entries([label_entry()]) :: labels()
  defp merge_entries(entries) do
    entries
    |> Enum.sort()
    |> Enum.reduce(%{}, fn {_original, name, value}, labels ->
      Map.update(labels, name, value, &(&1 <> ";" <> value))
    end)
  end

  @spec sanitize_name(String.t()) :: String.t()
  defp sanitize_name(name) do
    name = name |> String.replace(~r/[^a-zA-Z0-9_]+/, "_") |> String.replace(~r/_+/, "_")

    case name do
      <<first, _rest::binary>> when first in ?0..?9 -> "_" <> name
      _name -> name
    end
  end

  @spec label_value(term()) :: {:ok, String.t()} | :error
  defp label_value(value) when is_non_empty_binary(value) do
    if String.valid?(value), do: {:ok, value}, else: :error
  end

  defp label_value(value) when is_number(value) or is_boolean(value), do: {:ok, to_string(value)}
  defp label_value(:nan), do: {:ok, "NaN"}
  defp label_value(:infinity), do: {:ok, "Infinity"}
  defp label_value(:negative_infinity), do: {:ok, "-Infinity"}

  defp label_value(value) when is_list(value) or is_non_struct_map(value) do
    with {:ok, value} <- json_value(value),
         {:ok, encoded} <- Jason.encode(value) do
      {:ok, encoded}
    else
      _invalid -> :error
    end
  end

  defp label_value(_value), do: :error

  @spec json_value(term()) :: {:ok, term()} | :error
  defp json_value(value) when is_binary(value) do
    if String.valid?(value), do: {:ok, value}, else: :error
  end

  defp json_value(value) when is_number(value) or is_boolean(value) or is_nil(value),
    do: {:ok, value}

  defp json_value(value) when value in [:nan, :infinity, :negative_infinity],
    do: label_value(value)

  defp json_value(value) when is_list(value), do: json_list(value, [])

  defp json_value(value) when is_non_struct_map(value) do
    value
    |> Enum.reduce_while({:ok, []}, fn {key, value}, {:ok, entries} ->
      with {:ok, key} <- object_key(key),
           {:ok, value} <- json_value(value) do
        {:cont, {:ok, [{key, value} | entries]}}
      else
        _invalid -> {:halt, :error}
      end
    end)
    |> ordered_object()
  end

  defp json_value(_value), do: :error

  @spec object_key(term()) :: {:ok, String.t()} | :error
  defp object_key(key) when is_binary(key) or is_atom(key) do
    key = to_string(key)
    if String.valid?(key), do: {:ok, key}, else: :error
  end

  defp object_key(_key), do: :error

  @spec ordered_object({:ok, [{String.t(), term()}]} | :error) ::
          {:ok, OrderedObject.t()} | :error
  defp ordered_object({:ok, entries}), do: {:ok, OrderedObject.new(Enum.sort(entries))}
  defp ordered_object(:error), do: :error

  @spec json_list(term(), list()) :: {:ok, list()} | :error
  defp json_list([], values), do: {:ok, Enum.reverse(values)}

  defp json_list([value | rest], values) do
    with {:ok, value} <- json_value(value), do: json_list(rest, [value | values])
  end

  defp json_list(_improper_tail, _values), do: :error
end
