defmodule Logflare.Logs.OtelTest do
  use Logflare.DataCase

  import Logflare.Logs.Otel

  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.KeyValueList
  alias Opentelemetry.Proto.Resource.V1.Resource

  doctest Logflare.Logs.Otel

  describe "extract_value/1" do
    test "decodes map values without changing keys or empty values" do
      value = %AnyValue{
        value:
          {:kvlist_value,
           %KeyValueList{
             values: [
               %KeyValue{key: "nested.key", value: %AnyValue{value: {:int_value, 42}}},
               %KeyValue{key: "enabled", value: %AnyValue{value: {:bool_value, false}}},
               %KeyValue{key: "Key", value: %AnyValue{value: {:string_value, "upper"}}},
               %KeyValue{key: "key", value: %AnyValue{value: {:string_value, "lower"}}},
               %KeyValue{key: "empty", value: %AnyValue{}},
               %KeyValue{key: "map", value: %AnyValue{value: {:kvlist_value, %KeyValueList{}}}}
             ]
           }}
      }

      assert extract_value(value) == %{
               "nested.key" => 42,
               "enabled" => false,
               "Key" => "upper",
               "key" => "lower",
               "empty" => nil,
               "map" => %{}
             }
    end

    test "recursively decodes maps nested in arrays and arrays nested in maps" do
      value = %AnyValue{
        value:
          {:array_value,
           %ArrayValue{
             values: [
               %AnyValue{
                 value:
                   {:kvlist_value,
                    %KeyValueList{
                      values: [
                        %KeyValue{
                          key: "flags",
                          value: %AnyValue{
                            value:
                              {:array_value,
                               %ArrayValue{
                                 values: [
                                   %AnyValue{value: {:bool_value, true}},
                                   %AnyValue{value: {:bool_value, false}}
                                 ]
                               }}
                          }
                        },
                        %KeyValue{
                          key: "nested",
                          value: %AnyValue{
                            value:
                              {:kvlist_value,
                               %KeyValueList{
                                 values: [
                                   %KeyValue{
                                     key: "version",
                                     value: %AnyValue{value: {:string_value, "one"}}
                                   }
                                 ]
                               }}
                          }
                        }
                      ]
                    }}
               }
             ]
           }}
      }

      assert extract_value(value) == [
               %{"flags" => [true, false], "nested" => %{"version" => "one"}}
             ]
    end
  end

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
