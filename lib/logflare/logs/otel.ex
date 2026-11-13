defmodule Logflare.Logs.Otel do
  @moduledoc """
  Shared functionality between OtelMetrics and OtelTraces.
  """

  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Resource.V1.Resource

  @doc """
  Converts a Resource struct into a map

  ### Examples

      iex> handle_resource(%Resource{attributes: [%KeyValue{key: "service.name", value: "foo"}]})
      %{"service.name" => "foo"}
  """
  @spec handle_resource(Resource.t()) :: map()
  def handle_resource(%{attributes: attributes}) do
    Map.new(attributes, &extract_key_value/1)
  end

  def handle_resource(resource) when is_map(resource), do: %{}

  @doc """
  Extracts the project name from the *handled* resource

  By convention, that's the service name.
  """
  def resource_project(handled_resource) when is_non_struct_map(handled_resource) do
    handled_resource["service"]["name"] || handled_resource["service.name"]
  end

  @doc """
  Converts a InstrumentationScope to a map
  """
  @spec handle_scope(InstrumentationScope.t()) :: map()
  def handle_scope(scope) do
    %{name: name, version: version, attributes: scope_attributes} = scope

    %{
      "name" => name,
      "version" => version,
      "attributes" => handle_attributes(scope_attributes)
    }
  end

  @doc """
  Transforms a KeyValue struct into a tuple
  """
  @spec extract_key_value(KeyValue.t()) :: {String.t(), term()}
  def extract_key_value(%{key: key, value: nil}), do: {key, nil}

  def extract_key_value(%{key: key, value: value}) do
    {key, extract_value(value)}
  end

  @doc """
  Transforms AnyValue into simple elixir terms

  ### Examples

      iex> extract_value(%AnyValue{value: {:string_value, "foo"}})
      "foo"

      iex> extract_value(
      ...>   %AnyValue{value: {:array_value, %ArrayValue{values: [
      ...>     %AnyValue{value: {:string_value, "foo"}},
      ...>     %AnyValue{value: {:double_value, 0.3}},
      ...>   ]}}}
      ...> )
      ["foo", 0.3]

      iex> extract_value(123)
      123
  """
  @spec extract_value(AnyValue.t() | nil | term()) :: term()
  def extract_value(%{value: {:array_value, %{values: values}}}) do
    Enum.map(values, &extract_value/1)
  end

  def extract_value(%_{value: {_type, value}}), do: value
  def extract_value(%_{value: nil}), do: nil
  def extract_value(nil), do: nil
  def extract_value(value), do: value

  @doc """
  Transforms a list of KeyValue into a map
  """
  @spec handle_attributes([KeyValue.t()]) :: map()
  def handle_attributes(attributes) do
    Map.new(attributes, &extract_key_value/1)
  end
end
