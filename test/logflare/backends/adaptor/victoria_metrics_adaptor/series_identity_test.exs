defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.SeriesIdentityTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.SeriesIdentity
  alias Logflare.LogEvent
  alias Logflare.TestUtils
  alias Prometheus.WriteRequest

  test "keeps resource and scope variants in separate remote write series" do
    body = metric_body()

    bodies = [
      body,
      put_in(body, ["resource", "deployment_environment"], "staging"),
      put_in(body, ["scope", "name"], "library.b"),
      put_in(body, ["scope", "version"], "2"),
      put_in(body, ["scope", "attributes", "region"], "eu")
    ]

    series =
      bodies
      |> Enum.with_index(1)
      |> Enum.map(fn {body, value} -> event(Map.put(body, "value", value)) end)
      |> decode()

    assert length(series) == 5
    assert Enum.sort(for %{samples: [%{value: value}]} <- series, do: value) == [1, 2, 3, 4, 5]

    labels = Enum.map(series, &label_map/1)
    assert Enum.all?(labels, &(&1["job"] == "api" and &1["instance"] == "one"))
    assert labels |> Enum.map(& &1["logflare_resource_id"]) |> Enum.uniq() |> length() == 2
    assert labels |> Enum.map(& &1["logflare_scope_id"]) |> Enum.uniq() |> length() == 4
  end

  test "identical context still batches samples into one series" do
    body = metric_body()
    later = body |> Map.put("value", 2.0) |> Map.update!("timestamp", &(&1 + 1_000_000))

    assert [%{samples: [first, second]}] = decode([event(later), event(body)])
    assert first.value == 1.0
    assert second.value == 2.0
    assert second.timestamp - first.timestamp == 1_000
  end

  test "context IDs preserve composite values, scalar types, and array order" do
    values = [1, 1.0, "1", true, "true", nil, [], %{}, [1, 2], [2, 1], %{"a" => 1}, [["a", 1]]]

    for {context, label} <- [{"resource", "logflare_resource_id"}, {"scope", "logflare_scope_id"}] do
      ids =
        Enum.map(values, fn value ->
          SeriesIdentity.labels(%{context => %{"attribute" => value}})[label]
        end)

      assert MapSet.size(MapSet.new(ids)) == length(values)
      assert Enum.all?(ids, &Regex.match?(~r/\A[a-f0-9]{64}\z/, &1))
    end
  end

  test "context IDs are independent of map insertion order, including nested maps" do
    entries = for n <- 1..40, do: {"key_#{n}", n}
    first = Map.new(entries) |> Map.put("nested", %{"x" => 1, "y" => [2, 3]})

    second =
      entries |> Enum.reverse() |> Map.new() |> Map.put("nested", %{"y" => [2, 3], "x" => 1})

    assert SeriesIdentity.labels(%{"resource" => first, "scope" => first}) ==
             SeriesIdentity.labels(%{"resource" => second, "scope" => second})
  end

  test "context IDs use a stable canonical encoding" do
    assert SeriesIdentity.labels(%{"resource" => %{"env" => "prod"}}) == %{
             "logflare_resource_id" =>
               "636dae545c02c33adfe04be8199fb1ef0f76ccfe546c757333c6ab37925b22b6"
           }
  end

  test "context IDs preserve distinctions lost by label sanitization and stringification" do
    contexts = [
      %{"a.b" => "x"},
      %{"a_b" => "x"},
      %{"name" => "library", "attributes" => %{"name" => "first"}},
      %{"name" => "library", "attributes" => %{"name" => "second"}},
      %{"value" => :nan},
      %{"value" => "nan"},
      %{"value" => <<255>>}
    ]

    ids = Enum.map(contexts, &SeriesIdentity.labels(%{"scope" => &1})["logflare_scope_id"])
    assert MapSet.size(MapSet.new(ids)) == length(contexts)
  end

  test "generated identity and scope metadata cannot be overridden by configured labels" do
    body = put_in(metric_body(), ["scope", "schema_url"], "https://opentelemetry.io/schemas/1.0")
    generated = SeriesIdentity.labels(body)
    configured = Map.new(generated, fn {name, _value} -> {name, "configured"} end)
    attributes = Map.new(generated, fn {name, _value} -> {name, "point"} end)
    attributes = Map.put(attributes, "exported_logflare_scope_id", "already_exported")

    assert [series] =
             decode([event(Map.put(body, "attributes", attributes))], %{labels: configured})

    labels = label_map(series)
    assert Map.take(labels, Map.keys(generated)) == generated
    assert labels["otel_scope_name"] == "library.a"
    assert labels["otel_scope_version"] == "1"
    assert labels["otel_scope_schema_url"] == "https://opentelemetry.io/schemas/1.0"
    assert labels["exported_logflare_scope_id"] == "already_exported"
    assert labels["exported_exported_logflare_scope_id"] == "configured"
    assert labels["exported_otel_scope_name"] == "configured"
    assert labels["exported_logflare_resource_id"] == "configured"
  end

  test "absent or malformed outer contexts do not prevent a mixed batch from exporting" do
    for context <- [nil, %{}, [], "invalid", false, 1] do
      assert SeriesIdentity.labels(%{"resource" => context, "scope" => context}) == %{}
    end

    malformed = metric_body() |> Map.put("resource", "invalid") |> Map.put("scope", ["invalid"])
    assert length(decode([event(malformed), event(metric_body())])) == 2
  end

  test "rejects unsupported nested values and map keys without encoding them" do
    unsupported = [self(), make_ref(), fn -> :value end, <<1::1>>, [1 | 2]]

    for context <- ["resource", "scope"], value <- unsupported do
      assert :error =
               SeriesIdentity.labels(%{context => %{"attributes" => %{"nested" => [value]}}})

      assert :error = SeriesIdentity.labels(%{context => %{"attributes" => %{value => "value"}}})
      assert :error = SeriesIdentity.labels(%{context => %{"attributes" => {"nested", value}}})
    end
  end

  @tag capture_log: true
  test "drops malformed context events without losing valid samples in the batch" do
    telemetry_event = [:logflare, :backends, :victoria_metrics, :drop]
    TestUtils.attach_forwarder(telemetry_event)
    backend_id = System.unique_integer([:positive])
    valid = metric_body()

    invalid_resource = put_in(valid, ["resource", "nested"], %{"value" => self()})
    invalid_scope = put_in(valid, ["scope", "attributes", "nested"], [1 | 2])

    assert [%{samples: [%{value: 1.0}]}] =
             decode(
               [event(invalid_resource), event(valid), event(invalid_scope)],
               %{backend_id: backend_id}
             )

    assert_received {:telemetry_event, ^telemetry_event, %{count: 2},
                     %{reason: :invalid, backend_id: ^backend_id}}
  end

  @spec metric_body() :: map()
  defp metric_body do
    %{
      "event_message" => "requests",
      "metric_type" => "gauge",
      "value" => 1.0,
      "timestamp" => 1_700_000_000_000_000,
      "attributes" => %{},
      "resource" => %{
        "service.name" => "api",
        "service.instance.id" => "one",
        "deployment_environment" => "production"
      },
      "scope" => %{"name" => "library.a", "version" => "1", "attributes" => %{"region" => "us"}}
    }
  end

  @spec event(map()) :: LogEvent.t()
  defp event(body), do: %LogEvent{source_id: nil, event_type: :metric, body: body}

  @spec decode([LogEvent.t()], map()) :: [Prometheus.TimeSeries.t()]
  defp decode(events, config \\ %{}) do
    payload = VictoriaMetricsAdaptor.format_batch(events, config)
    {:ok, protobuf} = :snappyer.decompress(payload)
    WriteRequest.decode(protobuf).timeseries
  end

  @spec label_map(Prometheus.TimeSeries.t()) :: map()
  defp label_map(series), do: Map.new(series.labels, &{&1.name, &1.value})
end
