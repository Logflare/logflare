defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.OtelContextTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.OtelContext

  test "maps service attributes to the standard target labels" do
    assert OtelContext.target_labels(%{
             "service.name" => "checkout",
             "service.namespace" => "shop",
             "service.instance.id" => "one"
           }) == %{"job" => "shop/checkout", "instance" => "one"}

    assert OtelContext.target_labels(%{
             "_service_name" => "checkout",
             "_service_namespace" => "shop",
             "_service_instance_id" => "one"
           }) == %{"job" => "shop/checkout", "instance" => "one"}

    assert OtelContext.target_labels(%{"service.name" => "checkout"}) ==
             %{"job" => "checkout", "instance" => ""}

    assert OtelContext.target_labels(%{"service.name" => "checkout", "service.namespace" => ""}) ==
             %{"job" => "checkout", "instance" => ""}

    assert OtelContext.target_labels(%{"service.namespace" => "shop"}) == %{"instance" => ""}

    for resource <- [nil, %{}, [], "invalid", %URI{}] do
      assert OtelContext.target_labels(resource) == %{}
    end
  end

  test "sanitizes resource keys without removing the ingestion prefix" do
    assert OtelContext.resource_labels(%{
             "cloud.region" => "eu",
             "_host_name" => "host",
             "1.key" => "value",
             "a..b" => "first",
             "a__b" => "second",
             "a_b" => "third"
           }) == %{
             "cloud_region" => "eu",
             "_host_name" => "host",
             "_1_key" => "value",
             "a_b" => "first;second;third"
           }
  end

  test "converts scalar and composite attribute values to strings" do
    assert OtelContext.resource_labels(%{
             "bool" => false,
             "integer" => 42,
             "float" => 1.5,
             "nan" => :nan,
             "infinity" => :infinity,
             "negative_infinity" => :negative_infinity,
             "array" => [1, nil, true, :infinity, %{"b" => 2, "a" => 1}],
             "map" => %{"b" => [], "a" => %{"z" => 2, "a" => 1}},
             "empty_array" => [],
             "empty_map" => %{},
             "empty" => "",
             "null" => nil
           }) == %{
             "bool" => "false",
             "integer" => "42",
             "float" => "1.5",
             "nan" => "NaN",
             "infinity" => "Infinity",
             "negative_infinity" => "-Infinity",
             "array" => ~s([1,null,true,"Infinity",{"a":1,"b":2}]),
             "map" => ~s({"a":{"a":1,"z":2},"b":[]}),
             "empty_array" => "[]",
             "empty_map" => "{}"
           }
  end

  test "nested object label values are independent of map insertion order" do
    entries = for number <- 1..40, do: {"key_#{number}", number}
    first = Map.new(entries)
    second = Map.new(Enum.reverse(entries))

    assert OtelContext.resource_labels(%{"map" => first}) ==
             OtelContext.resource_labels(%{"map" => second})
  end

  test "skips malformed attributes without dropping valid context" do
    for value <- [self(), make_ref(), fn -> :ok end, {1, 2}, [1 | 2], <<255>>, %URI{}] do
      assert OtelContext.resource_labels(%{"good" => "yes", "bad" => value}) ==
               %{"good" => "yes"}

      assert OtelContext.resource_labels(%{"good" => "yes", "nested" => [value]}) ==
               %{"good" => "yes"}
    end

    assert OtelContext.resource_labels(%{"good" => "yes", {"bad", 1} => "invalid"}) ==
             %{"good" => "yes"}

    assert OtelContext.target_labels(%{"service.name" => <<255>>}) == %{"instance" => ""}
  end

  test "adds readable scope metadata and prefixed attributes" do
    scope = %{
      "name" => "library",
      "version" => "1.2",
      "schema_url" => "https://opentelemetry.io/schemas/1.0",
      "attributes" => %{"component.name" => "client", "1.key" => [true, nil]}
    }

    assert OtelContext.scope_labels(scope) == %{
             "otel_scope_name" => "library",
             "otel_scope_version" => "1.2",
             "otel_scope_schema_url" => "https://opentelemetry.io/schemas/1.0",
             "otel_scope_component_name" => "client",
             "otel_scope_1_key" => "[true,null]"
           }
  end

  test "drops scope attributes colliding with metadata even when metadata is absent" do
    assert OtelContext.scope_labels(%{
             "attributes" => %{
               "name" => "attribute",
               "version" => "attribute",
               "schema.url" => "attribute",
               "schema__url" => "attribute",
               "_name" => "attribute",
               "valid" => "yes"
             }
           }) == %{"otel_scope_valid" => "yes"}
  end

  test "joins scope and metric attribute collisions in original key order" do
    scope = %{"attributes" => %{"a.a" => "scope_a", "z.z" => "scope_z", "a_a" => "scope_b"}}
    points = %{"otel_scope_a_a" => "point_a", "otel_scope_z_z" => "point_z", "env" => "prod"}

    assert OtelContext.scope_labels(scope, points) == %{
             "otel_scope_a_a" => "scope_a;scope_b;point_a",
             "otel_scope_z_z" => "point_z;scope_z",
             "env" => "prod"
           }
  end

  test "retains metric attribute values that collide with scope metadata" do
    assert OtelContext.scope_labels(%{"name" => "library"}, %{"otel_scope_name" => "custom"}) ==
             %{"otel_scope_name" => "custom;library"}
  end

  test "omits invalid scope fields while retaining metric labels" do
    points = %{"env" => "prod"}

    for scope <- [nil, [], "invalid", %URI{}, %{"name" => <<255>>, "version" => 42}] do
      assert OtelContext.scope_labels(scope, points) == points
    end
  end
end
