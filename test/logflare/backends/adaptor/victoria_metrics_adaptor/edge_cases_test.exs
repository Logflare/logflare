defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.EdgeCasesTest do
  use Logflare.DataCase, async: false

  import Logflare.VictoriaMetricsTestHelpers

  alias Logflare.Backends
  alias Logflare.ContextCache
  alias Logflare.Sources

  @moduletag :victoria_metrics
  @vm_retry [sleep: 250, duration: 30_000]

  setup :setup_vm

  test "UTF-8 and escaped label values survive remote write and JSON export", %{source: source} do
    attributes = %{
      "unicode" => "東京 café 🚀",
      "escaped" => "quote=\" backslash=\\ newline=\n tab=\t nul=\0",
      "selector" => "{job=\"other\"}"
    }

    event = metric_event(source, metric_type: "gauge", value: 1, attributes: attributes)

    assert [series] = ingest([event], source)
    assert Map.take(series["metric"], Map.keys(attributes)) == attributes
    assert series["values"] == [1]
  end

  test "long metric names and label values cross protobuf length boundaries", %{source: source} do
    name = "long_" <> String.duplicate("metric", 200)
    value = String.duplicate("abcdefgh", 256)
    label_name = "label_" <> String.duplicate("x", 100)

    event =
      metric_event(source,
        event_message: name,
        metric_type: "gauge",
        value: 12.5,
        attributes: %{label_name => value}
      )

    assert [series] = ingest([event], source)
    assert series["metric"]["__name__"] == name
    assert series["metric"][label_name] == value
    assert series["values"] == [12.5]
  end

  test "VM's default label value limit drops the oversized series and preserves a control", %{
    source: source
  } do
    ignored_before = oversized_label_drops()

    oversized =
      metric_event(source,
        event_message: "over_limit",
        metric_type: "gauge",
        value: 1,
        attributes: %{"oversized" => String.duplicate("x", 8_192)}
      )

    control = metric_event(source, event_message: "kept", metric_type: "gauge", value: 9)

    assert {:ok, _} = Backends.ingest_logs([oversized, control], source)

    TestUtils.retry_assert(@vm_retry, fn ->
      assert oversized_label_drops() > ignored_before
      assert [series] = export(source)
      assert series["metric"]["__name__"] == "kept"
      assert series["values"] == [9]
    end)
  end

  test "empty and non-string metric names do not discard a valid control", %{
    source: source,
    backend: backend
  } do
    events =
      for name <- ["", 123, %{"name" => "invalid"}, ["invalid"]] do
        metric_event(source, event_message: name, metric_type: "gauge", value: 7)
      end

    control = metric_event(source, event_message: "kept", metric_type: "gauge", value: 9)

    assert [series] = ingest(events ++ [control], source)
    assert_drops(backend, %{invalid: 4})
    assert series["metric"]["__name__"] == "kept"
    assert series["values"] == [9]
  end

  test "valid metric colons and leading underscores retain their identity", %{source: source} do
    events =
      for {name, value} <- [{"service:requests:rate", 2}, {"_private", 3}] do
        metric_event(source, event_message: name, metric_type: "gauge", value: value)
      end

    assert ingest(events, source, 2) |> values_by_name() == %{
             "service:requests:rate" => [2],
             "_private" => [3]
           }
  end

  @tag config_labels: %{"_private" => "kept", "9node" => "worker", "deploy.region" => "eu"}
  test "configured names support a single underscore and normalize digits and punctuation", %{
    source: source
  } do
    event = metric_event(source, metric_type: "gauge", value: 1)

    assert [series] = ingest([event], source)

    assert %{"_private" => "kept", "_9node" => "worker", "deploy_region" => "eu"} =
             series["metric"]
  end

  test "one metric name retains distinct attribute label sets", %{source: source} do
    events =
      for {attributes, value} <- [
            {%{"queue" => "a"}, 1},
            {%{"queue" => "b"}, 2},
            {%{"queue" => "a", "worker" => "1"}, 3}
          ] do
        metric_event(source,
          event_message: "queue_depth",
          metric_type: "gauge",
          value: value,
          attributes: attributes
        )
      end

    series = ingest(events, source, 3)

    assert Map.new(series, &{Map.take(&1["metric"], ["queue", "worker"]), &1["values"]}) == %{
             %{"queue" => "a"} => [1],
             %{"queue" => "b"} => [2],
             %{"queue" => "a", "worker" => "1"} => [3]
           }
  end

  test "duplicate timestamps reach storage without choosing a winning value", %{source: source} do
    timestamp_ms = System.system_time(:millisecond) - 5_000

    events =
      for value <- [7, 9, 7] do
        metric_event(source,
          event_message: "duplicates",
          metric_type: "gauge",
          value: value,
          timestamp: timestamp_ms * 1_000_000
        )
      end

    assert {:ok, _} = Backends.ingest_logs(events, source)

    TestUtils.retry_assert(@vm_retry, fn ->
      assert [series] = export(source)
      assert series["timestamps"] == List.duplicate(timestamp_ms, 3)
      assert Enum.sort(series["values"]) == [7, 7, 9]
    end)
  end

  test "sub-millisecond samples retain their values when their timestamps collide", %{
    source: source
  } do
    timestamp_ms = System.system_time(:millisecond) - 5_000

    events =
      for {offset_ns, value} <- [{123_456, 1}, {987_654, 2}, {1_123_456, 3}] do
        metric_event(source,
          event_message: "precision",
          metric_type: "gauge",
          value: value,
          timestamp: timestamp_ms * 1_000_000 + offset_ns
        )
      end

    assert {:ok, _} = Backends.ingest_logs(events, source)

    TestUtils.retry_assert(@vm_retry, fn ->
      assert [series] = export(source)

      assert Enum.zip(series["timestamps"], series["values"]) |> Enum.sort() == [
               {timestamp_ms, 1},
               {timestamp_ms, 2},
               {timestamp_ms + 1, 3}
             ]
    end)
  end

  test "more than 250 events retain every gauge and an expanded histogram", %{source: source} do
    gauges =
      for index <- 1..251 do
        metric_event(source,
          event_message: "gauge_#{index}",
          metric_type: "gauge",
          value: index
        )
      end

    histogram =
      metric_event(source,
        event_message: "latency",
        metric_type: "histogram",
        aggregation_temporality: "cumulative",
        count: 3,
        sum: 4,
        bucket_counts: [1, 1, 1],
        explicit_bounds: [1, 2]
      )

    series = ingest(gauges ++ [histogram], source, 256)
    values = values_by_name(series)

    for index <- 1..251 do
      assert values["gauge_#{index}"] == [index]
    end

    assert values["latency_count"] == [3]
    assert values["latency_sum"] == [4]
    assert buckets(series, "latency_bucket") == %{"1" => [1], "2" => [2], "+Inf" => [3]}
  end

  test "renaming a source starts a new series while preserving its previous samples", %{
    source: source
  } do
    first = metric_event(source, event_message: "renamed", metric_type: "gauge", value: 1)
    assert [%{"values" => [1]}] = ingest([first], source)

    assert {:ok, renamed} = Sources.update_source(source, %{name: "vm_#{Ecto.UUID.generate()}"})
    assert {:ok, _} = ContextCache.bust_keys([{Sources, source.id}])

    second = metric_event(renamed, event_message: "renamed", metric_type: "gauge", value: 2)
    assert [series] = ingest([second], renamed)
    assert series["metric"]["source"] == renamed.name
    assert series["values"] == [2]
    assert [%{"values" => [1]}] = export(source)
  end

  @tag config_labels: %{"deployment" => "before"}
  test "updated backend labels apply to subsequent batches", %{source: source, backend: backend} do
    first = metric_event(source, event_message: "configured", metric_type: "gauge", value: 1)
    assert [before] = ingest([first], source)
    assert before["metric"]["deployment"] == "before"

    assert {:ok, _} =
             Backends.update_backend(backend, %{
               config: %{
                 url: vm_base_url() <> "/api/v1/write",
                 labels: %{"deployment" => "after"}
               }
             })

    assert {:ok, _} = ContextCache.bust_keys([{Backends, backend.id}])
    second = metric_event(source, event_message: "configured", metric_type: "gauge", value: 2)
    series = ingest([second], source, 2)

    assert Map.new(series, &{&1["metric"]["deployment"], &1["values"]}) == %{
             "before" => [1],
             "after" => [2]
           }
  end

  @spec oversized_label_drops() :: non_neg_integer()
  defp oversized_label_drops do
    assert {:ok, %{status_code: 200, body: body}} = HTTPoison.get(vm_base_url() <> "/metrics")

    case Regex.run(
           ~r/^vm_rows_ignored_total\{reason="too_long_label_value"\} (\d+)$/m,
           body,
           capture: :all_but_first
         ) do
      [count] -> String.to_integer(count)
      nil -> 0
    end
  end
end
