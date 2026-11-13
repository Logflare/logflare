defmodule Logflare.Logs.OtelTest do
  use Logflare.DataCase
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Resource.V1.Resource
  import Logflare.Logs.Otel

  doctest Logflare.Logs.Otel

  describe "handle_resource/1" do
    test "converts attributes to flat map with dot-notation keys" do
      attributes = [
        %KeyValue{key: "service.name", value: %AnyValue{value: {:string_value, "my-service"}}}
      ]

      result = handle_resource(%Resource{attributes: attributes})
      assert result == %{"service.name" => "my-service"}
    end

    test "handles multiple attributes with same prefix" do
      attributes = [
        %KeyValue{key: "service.name", value: %AnyValue{value: {:string_value, "my-service"}}},
        %KeyValue{key: "service.version", value: %AnyValue{value: {:string_value, "1.0.0"}}}
      ]

      result = handle_resource(%Resource{attributes: attributes})

      assert result == %{
               "service.name" => "my-service",
               "service.version" => "1.0.0"
             }
    end

    test "handles deeply nested dot-notation keys" do
      attributes = [
        %KeyValue{key: "a.b.c.d", value: %AnyValue{value: {:string_value, "deep"}}}
      ]

      result = handle_resource(%Resource{attributes: attributes})
      assert result == %{"a.b.c.d" => "deep"}
    end

    test "handles attributes with different branches" do
      attributes = [
        %KeyValue{key: "service.name", value: %AnyValue{value: {:string_value, "my-service"}}},
        %KeyValue{key: "service.version", value: %AnyValue{value: {:string_value, "1.0.0"}}},
        %KeyValue{
          key: "telemetry.sdk.name",
          value: %AnyValue{value: {:string_value, "opentelemetry"}}
        }
      ]

      result = handle_resource(%Resource{attributes: attributes})

      assert result == %{
               "service.name" => "my-service",
               "service.version" => "1.0.0",
               "telemetry.sdk.name" => "opentelemetry"
             }
    end

    test "handles empty attributes" do
      result = handle_resource(%Resource{attributes: []})
      assert result == %{}
    end
  end
end
