defmodule Logflare.Logs.OtelMetricTest do
  use Logflare.DataCase

  alias Logflare.Logs.OtelMetric

  describe "handle_batch/2" do
    setup do
      user = build(:user)
      source = insert(:source, user: user)
      request = TestUtilsGrpc.random_otel_metrics_request()
      %{resource_metrics: request.resource_metrics, source: source}
    end

    test "attributes", %{resource_metrics: resource_metrics, source: source} do
      [%{"attributes" => a} | _] = OtelMetric.handle_batch(resource_metrics, source)
      assert a != %{}
      assert Enum.any?(Map.values(a), &is_list/1)
      assert Enum.any?(Map.values(a), &is_number/1)
      assert Enum.any?(Map.values(a), &is_binary/1)
      assert Enum.any?(Map.values(a), &is_boolean/1)
    end

    test "creates events with correct metric_type for each metric type", %{
      resource_metrics: resource_metrics,
      source: source
    } do
      batch = OtelMetric.handle_batch(resource_metrics, source)

      metric_types = Enum.map(batch, & &1["metric_type"]) |> Enum.uniq() |> Enum.sort()
      assert metric_types == ["exponential_histogram", "gauge", "histogram", "sum"]
    end

    test "gauge metrics have expected fields", %{
      resource_metrics: resource_metrics,
      source: source
    } do
      batch = OtelMetric.handle_batch(resource_metrics, source)
      gauge = Enum.find(batch, &(&1["metric_type"] == "gauge"))

      assert gauge["event_message"]
      assert gauge["unit"]
      assert gauge["metadata"]["type"] == "metric"
      assert gauge["scope"]
      assert gauge["resource"]
      assert gauge["value"]
      assert gauge["start_time"]
      assert gauge["timestamp"]
      assert gauge["attributes"]
    end

    test "sum metrics have expected fields", %{resource_metrics: resource_metrics, source: source} do
      batch = OtelMetric.handle_batch(resource_metrics, source)
      sum = Enum.find(batch, &(&1["metric_type"] == "sum"))

      assert sum["event_message"]
      assert sum["unit"]
      assert sum["metadata"]["type"] == "metric"
      assert sum["aggregation_temporality"] in ["delta", "cumulative", "unspecified"]
      assert is_boolean(sum["is_monotonic"])
      assert sum["value"]
      assert sum["start_time"]
      assert sum["timestamp"]
    end

    test "histogram metrics have expected fields", %{
      resource_metrics: resource_metrics,
      source: source
    } do
      batch = OtelMetric.handle_batch(resource_metrics, source)
      histogram = Enum.find(batch, &(&1["metric_type"] == "histogram"))

      assert histogram["event_message"]
      assert histogram["unit"]
      assert histogram["metadata"]["type"] == "metric"
      assert histogram["aggregation_temporality"]
      assert histogram["count"]
      assert histogram["sum"]
      assert histogram["bucket_counts"]
      assert histogram["start_time"]
      assert histogram["timestamp"]
    end

    test "exponential_histogram metrics have expected fields", %{
      resource_metrics: resource_metrics,
      source: source
    } do
      batch = OtelMetric.handle_batch(resource_metrics, source)
      exp_histogram = Enum.find(batch, &(&1["metric_type"] == "exponential_histogram"))

      assert exp_histogram["event_message"]
      assert exp_histogram["unit"]
      assert exp_histogram["metadata"]["type"] == "metric"
      assert exp_histogram["aggregation_temporality"]
      assert exp_histogram["count"]
      assert exp_histogram["sum"]
      assert exp_histogram["scale"]
      assert exp_histogram["positive"]
      assert exp_histogram["negative"]
      assert exp_histogram["start_time"]
      assert exp_histogram["timestamp"]
    end

    test "json parsable log event body", %{resource_metrics: resource_metrics, source: source} do
      [params | _] = OtelMetric.handle_batch(resource_metrics, source)
      assert {:ok, _} = Jason.encode(params)
    end

    test "correctly parses resource metrics without structs or atoms", %{
      resource_metrics: resource_metrics,
      source: source
    } do
      converted = OtelMetric.handle_batch(resource_metrics, source)

      assert converted
             |> Iteraptor.each(
               fn
                 {_k, %_{}} ->
                   raise "contains struct"

                 {[:values | _], _v} ->
                   raise "contains atom"

                 {[:__unknown_fields__ | _], _v} ->
                   raise "contains atom"

                 self ->
                   self
               end,
               structs: :keep,
               yield: :all,
               keys: :reverse
             )
    end

    test "timestamps are unix nanoseconds", %{resource_metrics: resource_metrics, source: source} do
      [params | _] = OtelMetric.handle_batch(resource_metrics, source)

      assert is_integer(params["timestamp"])
      assert is_integer(params["start_time"])
      assert Integer.digits(params["timestamp"]) |> length() == 19
      assert Integer.digits(params["start_time"]) |> length() == 19
    end
  end
end
