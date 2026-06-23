defmodule Logflare.Logs.VectorGrpc do
  @moduledoc """
  Converts a list of `Event.EventWrapper` messages received from the Vector
  gRPC sink into Logflare events.

  Each `EventWrapper` carries one of:

    * `:log` — converted to a `vector_log` event. The `event_message` is
      derived from the log `value` when it is a string, or from a
      `"message"` field, otherwise an empty string.
    * `:metric` — converted to a `vector_metric` event. Fields are emitted in
      the OTEL-aligned shape expected by the ClickHouse metric mapping
      (`Logflare.Backends.Adaptor.ClickHouseAdaptor.MappingDefaults.for_metric/0`):
      a scalar `value`, an OTEL `metric_type` (`gauge`/`sum`/`histogram`/
      `summary`), `aggregation_temporality` derived from the Vector metric
      kind, and metric tags as `attributes`.
    * `:trace` — converted to a `vector_trace` event built from the proto
      `fields` map.
  """

  alias Event.EventWrapper
  alias Event.Log
  alias Event.Metric
  alias Event.Trace
  alias Event.Value

  @behaviour Logflare.Logs.Processor

  @impl true
  def handle_batch(events, _source) when is_list(events) do
    Enum.map(events, &handle_event/1)
  end

  defp handle_event(%EventWrapper{event: {:log, %Log{} = log}}), do: handle_log(log)

  defp handle_event(%EventWrapper{event: {:metric, %Metric{} = metric}}),
    do: handle_metric(metric)

  defp handle_event(%EventWrapper{event: {:trace, %Trace{} = trace}}), do: handle_trace(trace)
  defp handle_event(%EventWrapper{event: nil}), do: %{"metadata" => %{"type" => "vector_log"}}

  defp handle_log(%Log{value: value, fields: fields, metadata_full: metadata}) do
    value_term = extract_value(value)
    fields_map = handle_map(fields)

    %{
      "metadata" => %{"type" => "vector_log"},
      "event_message" => log_message(value_term, fields_map),
      "value" => value_term,
      "fields" => fields_map,
      "vector_metadata" => handle_metadata(metadata)
    }
  end

  defp handle_metric(%Metric{} = metric) do
    %{name: name, namespace: namespace, kind: kind} = metric
    sep = if namespace in [nil, ""], do: "", else: "."

    Map.merge(
      %{
        "metadata" => %{"type" => "vector_metric"},
        "event_message" => "#{namespace || ""}#{sep}#{name}",
        "name" => name,
        "namespace" => namespace,
        "aggregation_temporality" => kind_to_temporality(kind),
        "timestamp" => extract_timestamp(metric.timestamp),
        "attributes" => handle_tags_v1(metric.tags_v1),
        "interval_ms" => metric.interval_ms,
        "vector_metadata" => handle_metadata(metric.metadata_full)
      },
      metric_value_fields(metric.value)
    )
  end

  defp handle_trace(%Trace{fields: fields, metadata_full: metadata}) do
    fields_map = handle_map(fields)

    %{
      "metadata" => %{"type" => "vector_trace"},
      "event_message" => fields_map["message"] || fields_map["name"] || "vector_trace",
      "fields" => fields_map,
      "vector_metadata" => handle_metadata(metadata)
    }
  end

  defp log_message(value, fields) do
    cond do
      is_binary(value) -> value
      is_binary(fields["message"]) -> fields["message"]
      true -> ""
    end
  end

  # Vector's `Incremental`/`Absolute` map onto OTEL delta/cumulative
  # temporality. NOTE (open question): confirm this pairing with the team.
  defp kind_to_temporality(:Incremental), do: "delta"
  defp kind_to_temporality(:Absolute), do: "cumulative"
  defp kind_to_temporality(_), do: nil

  # Scalar Vector metric types map cleanly onto OTEL columns. Aggregate types
  # (histograms/summaries) are classified by family and carry count/sum where
  # available; full bucket/quantile fidelity and distribution/sketch handling
  # remain an open question (tracked on the PR).
  defp metric_value_fields(nil), do: %{}

  defp metric_value_fields({:counter, %{value: v}}),
    do: %{"metric_type" => "sum", "value" => v, "is_monotonic" => true}

  defp metric_value_fields({:gauge, %{value: v}}),
    do: %{"metric_type" => "gauge", "value" => v}

  defp metric_value_fields({:set, %{values: values}}),
    do: %{"metric_type" => "sum", "value" => length(values)}

  defp metric_value_fields({:aggregated_histogram1, %{buckets: buckets, counts: counts} = h}) do
    %{
      "metric_type" => "histogram",
      "count" => h.count,
      "sum" => h.sum,
      "explicit_bounds" => buckets,
      "bucket_counts" => counts
    }
  end

  defp metric_value_fields({:aggregated_summary1, %{quantiles: quantiles, values: values} = s}) do
    %{
      "metric_type" => "summary",
      "count" => s.count,
      "sum" => s.sum,
      "quantiles" => quantiles,
      "quantile_values" => values
    }
  end

  defp metric_value_fields({type, value}) do
    %{"metric_type" => metric_type_family(type)}
    |> maybe_put("count", Map.get(value, :count))
    |> maybe_put("sum", Map.get(value, :sum))
  end

  defp metric_type_family(type)
       when type in [:aggregated_histogram2, :aggregated_histogram3],
       do: "histogram"

  defp metric_type_family(type)
       when type in [:aggregated_summary2, :aggregated_summary3],
       do: "summary"

  defp metric_type_family(_), do: nil

  defp maybe_put(map, _key, nil), do: map
  defp maybe_put(map, key, value), do: Map.put(map, key, value)

  defp handle_tags_v1(tags) when is_map(tags), do: tags
  defp handle_tags_v1(_), do: %{}

  defp handle_map(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {k, extract_value(v)} end)
  end

  defp handle_map(_), do: %{}

  defp extract_value(nil), do: nil

  defp extract_value(%Value{kind: {:raw_bytes, bytes}}) when is_binary(bytes) do
    if String.valid?(bytes), do: bytes, else: Base.encode64(bytes)
  end

  defp extract_value(%Value{kind: {:timestamp, ts}}), do: extract_timestamp(ts)
  defp extract_value(%Value{kind: {:integer, v}}), do: v
  defp extract_value(%Value{kind: {:float, v}}), do: v
  defp extract_value(%Value{kind: {:boolean, v}}), do: v
  defp extract_value(%Value{kind: {:null, _}}), do: nil

  defp extract_value(%Value{kind: {:map, %{fields: fields}}}) when is_map(fields) do
    handle_map(fields)
  end

  defp extract_value(%Value{kind: {:array, %{items: items}}}) when is_list(items) do
    Enum.map(items, &extract_value/1)
  end

  defp extract_value(%Value{kind: nil}), do: nil
  defp extract_value(other), do: other

  defp extract_timestamp(%Google.Protobuf.Timestamp{seconds: s, nanos: n}) do
    s * 1_000_000_000 + n
  end

  defp extract_timestamp(_), do: nil

  defp handle_metadata(nil), do: %{}

  defp handle_metadata(%Event.Metadata{} = m) do
    %{
      "value" => extract_value(m.value),
      "source_id" => m.source_id,
      "source_type" => m.source_type
    }
  end
end
