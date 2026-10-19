defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.ResourceMetadataTest do
  use ExUnit.Case, async: true

  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
  alias Logflare.LogEvent
  alias Prometheus.WriteRequest

  test "exports resource metadata only on target_info and scope metadata on metric points" do
    event = event(%{"attributes" => %{"method" => "GET"}})
    series = decode([event], %{labels: %{"deployment" => "prod"}})
    assert [metric] = named(series, "requests")
    assert [target] = named(series, "target_info")

    assert labels(metric) == %{
             "__name__" => "requests",
             "source" => "unknown",
             "job" => "shop/api",
             "instance" => "replica-a",
             "method" => "GET",
             "deployment" => "prod",
             "otel_scope_name" => "library",
             "otel_scope_version" => "1",
             "otel_scope_region" => "eu"
           }

    assert labels(target) == %{
             "__name__" => "target_info",
             "job" => "shop/api",
             "instance" => "replica-a",
             "service_name" => "api",
             "service_namespace" => "shop",
             "service_instance_id" => "replica-a",
             "host_name" => "host-a"
           }

    assert [%{value: 1.0, timestamp: 1_700_000_000_000}] = target.samples
  end

  test "exports one latest resource per target regardless of batch order or scope" do
    first = event(%{})

    later =
      first
      |> put_in([Access.key(:body), "resource", "host.name"], "host-b")
      |> put_in([Access.key(:body), "timestamp"], 1_700_000_001_000_000)

    other_scope = put_in(later.body["scope"]["name"], "another.library")

    for events <- [[first, later, other_scope], [other_scope, later, first]] do
      series = decode(events)
      assert [target] = named(series, "target_info")
      assert labels(target)["host_name"] == "host-b"
      assert [%{value: 1.0, timestamp: 1_700_000_001_000}] = target.samples
      assert length(named(series, "requests")) == 2
    end
  end

  test "target_info has a deterministic tie break for conflicting metadata at one timestamp" do
    first = event(%{})
    second = put_in(first.body["resource"]["host.name"], "host-b")

    assert named(decode([first, second]), "target_info") ==
             named(decode([second, first]), "target_info")

    assert length(named(decode([first, second]), "target_info")) == 1
  end

  test "metadata changes do not invent a new identity for the same job and instance" do
    first = event(%{})

    second =
      first
      |> put_in([Access.key(:body), "resource", "host.name"], "host-b")
      |> put_in([Access.key(:body), "timestamp"], 1_700_000_001_000_000)
      |> put_in([Access.key(:body), "value"], 2.0)

    assert [metric] = named(decode([second, first]), "requests")
    assert Enum.map(metric.samples, & &1.value) == [1.0, 2.0]
    refute Map.has_key?(labels(metric), "logflare_resource_id")
    refute Map.has_key?(labels(metric), "logflare_scope_id")
  end

  test "distinct instances have separate metrics and target_info series" do
    first = event(%{})
    second = put_in(first.body["resource"]["service.instance.id"], "replica-b")
    series = decode([first, second])

    for name <- ["requests", "target_info"] do
      assert named(series, name) |> Enum.map(&labels(&1)["instance"]) |> Enum.sort() ==
               ["replica-a", "replica-b"]
    end
  end

  test "does not emit unidentifiable or empty targets" do
    for resource <- [nil, %{}, %{"host.name" => "host-a"}] do
      assert [metric] = decode([event(%{"resource" => resource})])
      assert labels(metric)["__name__"] == "requests"
    end
  end

  @tag capture_log: true
  test "dropped points do not emit resource metadata" do
    assert [] = decode([event(%{"value" => "invalid"})])
  end

  test "scope metadata collisions concatenate values instead of exporting a custom label" do
    series = decode([event(%{"attributes" => %{"otel_scope_name" => "custom"}})])
    assert [metric] = named(series, "requests")
    assert labels(metric)["otel_scope_name"] == "custom;library"
    refute Map.has_key?(labels(metric), "exported_otel_scope_name")
  end

  test "scope collision ordering uses point attribute keys before label sanitization" do
    event = event(%{"attributes" => %{"otel.scope.name" => "zzz"}})
    event = put_in(event.body["scope"]["name"], "aaa")
    assert [metric] = named(decode([event]), "requests")
    assert labels(metric)["otel_scope_name"] == "zzz;aaa"
  end

  test "point attribute normalization collisions retain values in original-key order" do
    event = event(%{"attributes" => %{"a.b" => "first", "a_b" => "second"}})
    assert [metric] = named(decode([event]), "requests")
    assert labels(metric)["a_b"] == "first;second"
  end

  @spec event(map()) :: LogEvent.t()
  defp event(overrides) do
    body = %{
      "event_message" => "requests",
      "metric_type" => "gauge",
      "value" => 1.0,
      "timestamp" => 1_700_000_000_000_000,
      "resource" => %{
        "service.name" => "api",
        "service.namespace" => "shop",
        "service.instance.id" => "replica-a",
        "host.name" => "host-a"
      },
      "scope" => %{
        "name" => "library",
        "version" => "1",
        "attributes" => %{"region" => "eu"}
      }
    }

    %LogEvent{source_id: nil, event_type: :metric, body: Map.merge(body, overrides)}
  end

  @spec decode([LogEvent.t()], map()) :: [Prometheus.TimeSeries.t()]
  defp decode(events, config \\ %{}) do
    payload = VictoriaMetricsAdaptor.format_batch(events, config)
    {:ok, protobuf} = :snappyer.decompress(payload)
    WriteRequest.decode(protobuf).timeseries
  end

  @spec named([Prometheus.TimeSeries.t()], String.t()) :: [Prometheus.TimeSeries.t()]
  defp named(series, name), do: Enum.filter(series, &(labels(&1)["__name__"] == name))

  @spec labels(Prometheus.TimeSeries.t()) :: map()
  defp labels(series), do: Map.new(series.labels, &{&1.name, &1.value})
end
