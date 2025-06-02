defmodule Logflare.Logs.Otel do
  @moduledoc """
  Shared functionality between OtelMetrics and OtelTraces.
  """

  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue

  def handle_resource(%{attributes: attributes}) do
    Enum.reduce(attributes, %{}, fn attribute, acc ->
      {k,v} = extract_key_value(attribute)
      k =  String.split(k, ".") |> Enum.reverse()
      map = Enum.reduce(k, v, fn key, acc -> %{key => acc} end)
      DeepMerge.deep_merge(map, acc)
    end)
  end

  def merge_scope_attributes(resource, scope) do
    %{name: name, version: version, attributes: scope_attributes} = scope

    resource
    |> Map.merge(%{"name" => name, "version" => version})
    |> Map.merge(handle_attributes(scope_attributes))
  end

  def extract_key_value(%KeyValue{key: key, value: nil}), do: {key, nil}

  def extract_key_value(%KeyValue{
         key: key,
         value: value
       }) do
    {key, extract_value(value)}
  end

  def extract_value(%AnyValue{value: {:array_value, %ArrayValue{values: values}}}) do
    Enum.map(values, &extract_value/1)
  end

  def extract_value(%_{value: {_type, value}}), do: value
  def extract_value(nil), do: nil
  def extract_value(value), do: value

  # TODO: rename
  def handle_attributes(attributes) do
    Map.new(attributes, &extract_key_value/1)
  end

  def nano_to_iso8601(time_nano) do
    time_nano
    |> DateTime.from_unix!(:nanosecond)
    |> DateTime.to_iso8601()
  end
end
