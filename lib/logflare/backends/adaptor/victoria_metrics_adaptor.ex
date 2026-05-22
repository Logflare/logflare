defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor do
  @moduledoc """
  Backend adaptor for VictoriaMetrics using Prometheus remote write protocol (v0.1.0).

  Accepts only metric-type events (body["metadata"]["type"] == "metric"). Log and
  trace events are silently dropped in format_batch/1. Exponential histogram events
  are also dropped as Prometheus remote write v1 has no native representation for them.

  The payload is protobuf-encoded and snappy block-compressed before POST. The URL
  should point to the VictoriaMetrics remote-write endpoint, e.g.
  http://victoriametrics:8428/api/v1/write.

  Optional `labels` config map is merged into every time series (config wins over
  event attributes on key collision).
  """

  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Sources
  alias Logflare.Utils

  @behaviour Logflare.Backends.Adaptor

  def child_spec(arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [arg]}}
  end

  @impl Logflare.Backends.Adaptor
  def start_link({source, backend}) do
    backend = %{backend | config: transform_config(backend)}
    WebhookAdaptor.start_link({source, backend})
  end

  @impl Logflare.Backends.Adaptor
  @spec format_batch(list()) :: binary()
  def format_batch(log_events) do
    timeseries =
      log_events
      |> Enum.filter(&metric_event?/1)
      |> Enum.flat_map(&to_timeseries/1)

    encode_write_request(%Prometheus.WriteRequest{timeseries: timeseries})
  end

  @impl Logflare.Backends.Adaptor
  def transform_config(%_{config: config}) do
    basic_auth = Utils.encode_basic_auth(config)

    headers =
      Map.get(config, :headers, %{})
      |> Map.put("Content-Type", "application/x-protobuf")
      |> Map.put("Content-Encoding", "snappy")
      |> Map.put("X-Prometheus-Remote-Write-Version", "0.1.0")
      |> then(fn h ->
        if basic_auth, do: Map.put(h, "Authorization", "Basic #{basic_auth}"), else: h
      end)

    %{
      url: config.url,
      headers: headers,
      format_batch: &format_batch/1,
      gzip: false,
      http: "http1"
    }
  end

  @impl Logflare.Backends.Adaptor
  def cast_config(params, existing_config \\ %{}) do
    {existing_config,
     %{url: :string, headers: :map, username: :string, password: :string, labels: :map}}
    |> Ecto.Changeset.cast(params, [:url, :headers, :username, :password, :labels])
  end

  @impl Logflare.Backends.Adaptor
  def validate_config(changeset) do
    import Ecto.Changeset

    changeset
    |> validate_required([:url])
    |> validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_user_pass()
  end

  @impl Logflare.Backends.Adaptor
  def redact_config(config) do
    if Map.get(config, :password) do
      Map.put(config, :password, "REDACTED")
    else
      config
    end
  end

  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    backend = %{backend | config: transform_config(backend)}
    empty_body = encode_write_request(%Prometheus.WriteRequest{timeseries: []})
    WebhookAdaptor.test_connection(backend, empty_body)
  end

  # --- private helpers ---

  defp encode_write_request(%Prometheus.WriteRequest{} = req) do
    {:ok, compressed} = req |> Prometheus.WriteRequest.encode() |> :snappyer.compress()
    compressed
  end

  defp metric_event?(%{body: %{"metadata" => %{"type" => "metric"}}}), do: true
  defp metric_event?(_), do: false

  defp to_timeseries(%{body: body, source_id: source_id}) do
    metric_name = sanitize_metric_name(body["event_message"])
    timestamp_ms = nano_to_ms(body["timestamp"])

    source_label =
      case Sources.Cache.get_by_id(source_id) do
        %{name: name} -> name
        _ -> "unknown"
      end

    base_labels = %{"source" => source_label} |> Map.merge(flat_attributes(body["attributes"]))

    case body["metric_type"] do
      type when type in ["gauge", "sum"] ->
        value = to_float(body["value"])
        labels = Map.put(base_labels, "__name__", metric_name) |> to_label_list()

        [
          %Prometheus.TimeSeries{
            labels: labels,
            samples: [%Prometheus.Sample{value: value, timestamp: timestamp_ms}]
          }
        ]

      "histogram" ->
        histogram_series(metric_name, base_labels, body, timestamp_ms)

      _ ->
        []
    end
  end

  defp histogram_series(name, base_labels, body, ts_ms) do
    count = to_float(body["count"] || 0)
    sum = to_float(body["sum"] || 0.0)
    bucket_counts = body["bucket_counts"] || []
    explicit_bounds = body["explicit_bounds"] || []

    count_ts = %Prometheus.TimeSeries{
      labels: Map.put(base_labels, "__name__", name <> "_count") |> to_label_list(),
      samples: [%Prometheus.Sample{value: count, timestamp: ts_ms}]
    }

    sum_ts = %Prometheus.TimeSeries{
      labels: Map.put(base_labels, "__name__", name <> "_sum") |> to_label_list(),
      samples: [%Prometheus.Sample{value: sum, timestamp: ts_ms}]
    }

    {bucket_series, _cumulative} =
      bucket_counts
      |> Enum.with_index()
      |> Enum.map_reduce(0, fn {count, idx}, cumulative ->
        cumulative = cumulative + count
        le = if idx < length(explicit_bounds), do: to_string(Enum.at(explicit_bounds, idx)), else: "+Inf"

        ts = %Prometheus.TimeSeries{
          labels:
            Map.merge(base_labels, %{"__name__" => name <> "_bucket", "le" => le})
            |> to_label_list(),
          samples: [%Prometheus.Sample{value: to_float(cumulative), timestamp: ts_ms}]
        }

        {ts, cumulative}
      end)

    [count_ts, sum_ts | bucket_series]
  end

  defp flat_attributes(nil), do: %{}

  defp flat_attributes(attrs) when is_map(attrs) do
    for {k, v} <- attrs,
        is_binary(k),
        into: %{} do
      {sanitize_label_name(k), to_string(v)}
    end
  end

  defp flat_attributes(_), do: %{}

  defp to_label_list(label_map) do
    label_map
    |> Enum.sort_by(fn {k, _} -> k end)
    |> Enum.map(fn {name, value} ->
      %Prometheus.Label{name: name, value: to_string(value)}
    end)
  end

  defp sanitize_metric_name(nil), do: "unknown"

  defp sanitize_metric_name(name) when is_binary(name) do
    name
    |> String.replace(~r/[^a-zA-Z0-9_:]/, "_")
    |> then(fn n ->
      if String.match?(n, ~r/^[^a-zA-Z_:]/), do: "_" <> n, else: n
    end)
  end

  defp sanitize_label_name(name) when is_binary(name) do
    name
    |> String.replace(~r/[^a-zA-Z0-9_]/, "_")
    |> then(fn n ->
      if String.match?(n, ~r/^[^a-zA-Z_]/), do: "_" <> n, else: n
    end)
  end

  defp to_float(v) when is_float(v), do: v
  defp to_float(v) when is_integer(v), do: v * 1.0
  defp to_float(_), do: 0.0

  defp nano_to_ms(nil), do: 0
  defp nano_to_ms(ns) when is_integer(ns), do: div(ns, 1_000_000)
  defp nano_to_ms(_), do: 0

  defp validate_user_pass(changeset) do
    user = Ecto.Changeset.get_field(changeset, :username)
    pass = Ecto.Changeset.get_field(changeset, :password)

    if [user, pass] != [nil, nil] and Enum.any?([user, pass], &is_nil/1) do
      msg = "Both username and password must be provided for basic auth"

      changeset
      |> Ecto.Changeset.add_error(:username, msg)
      |> Ecto.Changeset.add_error(:password, msg)
    else
      changeset
    end
  end
end
