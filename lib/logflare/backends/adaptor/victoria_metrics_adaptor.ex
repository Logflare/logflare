defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor do
  @moduledoc """
  Backend adaptor for VictoriaMetrics using Prometheus remote write protocol (v0.1.0).

  The payload is protobuf-encoded and snappy block-compressed before POST. The URL
  should point to the VictoriaMetrics remote-write endpoint, e.g.
  http://victoriametrics:8428/api/v1/write.

  Only metric-type events (body["metadata"]["type"] == "metric") are sent. Gauges,
  sums and explicit-bucket histograms are supported. These are dropped, with a
  warning log and a `[:logflare, :backends, :victoria_metrics, :drop]` telemetry
  event per reason:

    * log and trace events (`:not_a_metric`)
    * exponential histograms and other types remote write v1 cannot represent
      (`:unsupported_type`)
    * monotonic sums and histograms that are not cumulative (`:non_cumulative`),
      since remote write has no delta representation

  Labels on each series:

    * `source` - the source name
    * `job` and `instance` - from the `service.namespace`/`service.name` and
      `service.instance.id` resource attributes, when present
    * data point attributes with string, number or boolean values; list and map
      values are skipped. Names are as stored by Logflare, which normalizes keys
      at ingest (e.g. `http.route` becomes `_http_route`)
    * the optional `labels` config map, which wins over attributes on collision

  Labels the adaptor sets (`__name__`, `le`, `source`, `job`, `instance`) always win.
  An attribute or config label with one of those names is kept as `exported_<name>`.
  """

  @behaviour Logflare.Backends.Adaptor

  require Logger

  alias Logflare.Backends.Adaptor.HttpBased.Headers
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWrite
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Utils

  @reserved_labels ["__name__", "le", "source", "job", "instance"]
  @redacted_value Headers.redacted_value()

  @type drop_reason :: :not_a_metric | :unsupported_type | :non_cumulative

  defguardp is_label_char(c) when c in ?a..?z or c in ?A..?Z or c in ?0..?9 or c == ?_

  @spec child_spec({Source.t(), Backend.t()}) :: Supervisor.child_spec()
  def child_spec(arg) do
    %{id: __MODULE__, start: {__MODULE__, :start_link, [arg]}}
  end

  @impl Logflare.Backends.Adaptor
  @spec start_link({Source.t(), Backend.t()}) :: GenServer.on_start()
  def start_link({source, %Backend{} = backend}) do
    WebhookAdaptor.start_link({source, %{backend | config: transform_config(backend)}})
  end

  @impl Logflare.Backends.Adaptor
  @spec format_batch([LogEvent.t()]) :: binary()
  def format_batch(log_events), do: format_batch(log_events, %{})

  @impl Logflare.Backends.Adaptor
  @spec format_batch([LogEvent.t()], map()) :: binary()
  def format_batch(log_events, config) do
    context = %{
      source_names: source_names(log_events),
      static_labels: flat_labels(Map.get(config, :labels))
    }

    {series, drops} =
      Enum.reduce(log_events, {%{}, %{}}, fn event, {series, drops} ->
        case labeled_samples(event, context) do
          {:ok, labeled} -> {Enum.reduce(labeled, series, &add_sample/2), drops}
          {:drop, reason} -> {series, Map.update(drops, reason, 1, &(&1 + 1))}
        end
      end)

    report_drops(drops, length(log_events), Map.get(config, :backend_id))

    series
    |> Enum.map(fn {labels, samples} -> {labels, Enum.sort_by(samples, &elem(&1, 1))} end)
    |> encode_write_request()
  end

  @impl Logflare.Backends.Adaptor
  @spec transform_config(Backend.t()) :: map()
  def transform_config(%_{config: config} = backend) do
    headers =
      (Map.get(config, :headers) || %{})
      |> Headers.normalize_keys()
      |> Map.merge(%{
        "content-type" => "application/x-protobuf",
        "content-encoding" => "snappy",
        "x-prometheus-remote-write-version" => "0.1.0"
      })
      |> put_basic_auth(Utils.encode_basic_auth(config))

    batch_config = %{labels: Map.get(config, :labels), backend_id: Map.get(backend, :id)}

    %{
      url: config.url,
      headers: headers,
      format_batch: &format_batch(&1, batch_config),
      gzip: false,
      http: "http1"
    }
  end

  @impl Logflare.Backends.Adaptor
  @spec cast_config(map(), map()) :: Ecto.Changeset.t()
  def cast_config(params, existing_config \\ %{}) do
    {existing_config,
     %{url: :string, headers: :map, username: :string, password: :string, labels: :map}}
    |> Ecto.Changeset.cast(params, [:url, :headers, :username, :password, :labels])
    |> WebhookAdaptor.unredact_credentials(existing_config)
    |> unredact_password()
  end

  @impl Logflare.Backends.Adaptor
  @spec validate_config(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> validate_user_pass()
    |> WebhookAdaptor.validate_no_ssrf()
  end

  @impl Logflare.Backends.Adaptor
  @spec redact_config(map()) :: map()
  def redact_config(config) do
    config
    |> WebhookAdaptor.redact_config()
    |> Map.replace_lazy(:password, &Headers.mask_value/1)
  end

  @impl Logflare.Backends.Adaptor
  @spec test_connection(Backend.t()) :: :ok | {:error, term()}
  def test_connection(%Backend{} = backend) do
    backend = %{backend | config: transform_config(backend)}
    empty_body = encode_write_request([])
    WebhookAdaptor.test_connection(backend, empty_body)
  end

  defp put_basic_auth(headers, nil), do: headers
  defp put_basic_auth(headers, encoded), do: Map.put(headers, "authorization", "Basic #{encoded}")

  # A client echoing the redacted password back on update keeps the stored one.
  defp unredact_password(changeset) do
    if Ecto.Changeset.get_change(changeset, :password) == @redacted_value do
      Ecto.Changeset.delete_change(changeset, :password)
    else
      changeset
    end
  end

  defp encode_write_request(series) do
    {:ok, compressed} = series |> RemoteWrite.encode() |> :snappyer.compress()
    compressed
  end

  defp add_sample({labels, sample}, series) do
    Map.update(series, labels, [sample], &[sample | &1])
  end

  defp source_names(log_events) do
    log_events
    |> Enum.map(& &1.source_id)
    |> Enum.uniq()
    |> Map.new(&{&1, source_name(&1)})
  end

  defp source_name(source_id) when is_integer(source_id) do
    case Sources.Cache.get_by_id(source_id) do
      %{name: name} -> name
      _ -> "unknown"
    end
  end

  defp source_name(_source_id), do: "unknown"

  defp labeled_samples(%{body: %{"metadata" => %{"type" => "metric"}} = body} = event, context) do
    with {:ok, kind} <- series_kind(body) do
      name = sanitize_name(body["event_message"] || "unknown", true)
      labels = series_labels(body, Map.get(context.source_names, event.source_id), context)
      {:ok, samples(kind, name, labels, body, micro_to_ms(body["timestamp"]))}
    end
  end

  defp labeled_samples(_event, _context), do: {:drop, :not_a_metric}

  defp series_kind(%{"metric_type" => "gauge"}), do: {:ok, :value}

  defp series_kind(%{"metric_type" => "sum", "is_monotonic" => true} = body),
    do: cumulative_kind(body, :value)

  defp series_kind(%{"metric_type" => "sum"}), do: {:ok, :value}
  defp series_kind(%{"metric_type" => "histogram"} = body), do: cumulative_kind(body, :histogram)
  defp series_kind(_body), do: {:drop, :unsupported_type}

  # Events without a temporality did not come from OTLP and are taken as cumulative.
  defp cumulative_kind(%{"aggregation_temporality" => temporality}, _kind)
       when temporality not in [nil, "cumulative"],
       do: {:drop, :non_cumulative}

  defp cumulative_kind(_body, kind), do: {:ok, kind}

  # `labels` is sorted by name and never holds `__name__` or `le`, so inserting them
  # keeps every series' label list in the order remote write requires.
  defp samples(:value, name, labels, body, ts_ms) do
    [{insert_label(labels, "__name__", name), sample(body["value"], ts_ms)}]
  end

  defp samples(:histogram, name, labels, body, ts_ms) do
    bounds = Enum.map(body["explicit_bounds"] || [], &format_float/1) ++ ["+Inf"]
    bucket_labels = insert_label(labels, "__name__", name <> "_bucket")

    {buckets, _cumulative} =
      (body["bucket_counts"] || [])
      |> Enum.zip(bounds)
      |> Enum.map_reduce(0, fn {count, le}, cumulative ->
        cumulative = cumulative + count
        {{insert_label(bucket_labels, "le", le), sample(cumulative, ts_ms)}, cumulative}
      end)

    [
      {insert_label(labels, "__name__", name <> "_count"), sample(body["count"] || 0, ts_ms)},
      {insert_label(labels, "__name__", name <> "_sum"), sample(body["sum"] || 0, ts_ms)}
      | buckets
    ]
  end

  defp sample(value, ts_ms), do: {to_float(value), ts_ms}

  defp insert_label([{key, _value} = label | rest], name, value) when key < name,
    do: [label | insert_label(rest, name, value)]

  defp insert_label(labels, name, value), do: [{name, value} | labels]

  defp series_labels(body, source_name, context) do
    adaptor_labels =
      body["resource"]
      |> resource_labels()
      |> Map.put("source", source_name || "unknown")

    body["attributes"]
    |> flat_labels()
    |> Map.merge(context.static_labels)
    |> export_reserved()
    |> Map.merge(adaptor_labels)
    |> Enum.sort()
  end

  # LogEvent.make/2 stores keys in BigQuery column form, so `service.name` arrives as
  # `_service_name`. Both forms are accepted.
  defp resource_labels(%{} = resource) do
    name = string_attr(resource, "service.name", "_service_name")
    namespace = string_attr(resource, "service.namespace", "_service_namespace")
    instance = string_attr(resource, "service.instance.id", "_service_instance_id")
    job = if name && namespace, do: namespace <> "/" <> name, else: name

    for {key, value} <- [{"job", job}, {"instance", instance}], value != nil, into: %{} do
      {key, value}
    end
  end

  defp resource_labels(_resource), do: %{}

  defp string_attr(map, key, normalized_key) do
    case Map.get(map, key) || Map.get(map, normalized_key) do
      value when is_binary(value) and value != "" -> value
      _ -> nil
    end
  end

  defp export_reserved(labels) do
    Enum.reduce(@reserved_labels, labels, fn name, acc ->
      case Map.pop(acc, name) do
        {nil, acc} -> acc
        {value, acc} -> Map.put(acc, "exported_" <> name, value)
      end
    end)
  end

  defp flat_labels(labels) when is_map(labels) do
    for {key, value} <- labels,
        is_binary(key) or is_atom(key),
        is_binary(value) or is_number(value) or is_boolean(value),
        into: %{} do
      {sanitize_name(to_string(key), false), label_value(value)}
    end
  end

  defp flat_labels(_labels), do: %{}

  defp label_value(value) when is_float(value), do: format_float(value)
  defp label_value(value), do: to_string(value)

  # Prometheus identifiers are [a-zA-Z_][a-zA-Z0-9_]*, plus `:` in metric names. Other
  # bytes, including each byte of a multi-byte character, become `_`.
  defp sanitize_name(name, colon?) do
    sanitized = if valid_name?(name, colon?), do: name, else: replace_invalid(name, colon?)

    case sanitized do
      <<c, _rest::binary>> when c in ?0..?9 -> "_" <> sanitized
      _ -> sanitized
    end
  end

  defp valid_name?(<<c, rest::binary>>, colon?) when is_label_char(c),
    do: valid_name?(rest, colon?)

  defp valid_name?(<<?:, rest::binary>>, true), do: valid_name?(rest, true)
  defp valid_name?(<<>>, _colon?), do: true
  defp valid_name?(_name, _colon?), do: false

  defp replace_invalid(name, colon?) do
    for <<c <- name>>, into: "" do
      if is_label_char(c) or (colon? and c == ?:), do: <<c>>, else: "_"
    end
  end

  # Matches the plain decimal form Prometheus uses for `le` values, e.g. "1000" and
  # "0.005", where Float.to_string/1 would give "1.0e3".
  defp format_float(value) when is_integer(value), do: Integer.to_string(value)

  defp format_float(value) when abs(value) < 1.0e15 do
    integer = trunc(value)
    if integer == value, do: Integer.to_string(integer), else: format_fraction(value)
  end

  defp format_float(value), do: format_fraction(value)

  defp format_fraction(value) do
    short = :erlang.float_to_binary(value, [:short])

    if String.contains?(short, "e"),
      do: value |> Decimal.from_float() |> Decimal.to_string(:normal),
      else: short
  end

  defp to_float(v) when is_float(v), do: v
  defp to_float(v) when is_integer(v), do: v * 1.0
  defp to_float(_), do: 0.0

  # LogEvent.make/2 normalizes body["timestamp"] to microseconds.
  defp micro_to_ms(us) when is_integer(us), do: div(us, 1_000)
  defp micro_to_ms(_), do: System.system_time(:millisecond)

  @spec report_drops(%{drop_reason() => pos_integer()}, non_neg_integer(), term()) :: :ok
  defp report_drops(drops, _total, _backend_id) when map_size(drops) == 0, do: :ok

  defp report_drops(drops, total, backend_id) do
    dropped = Enum.reduce(drops, 0, fn {_reason, count}, acc -> acc + count end)

    Logger.warning(
      "Dropping #{dropped} of #{total} VictoriaMetrics event(s): #{inspect(drops)}",
      backend_id: backend_id
    )

    for {reason, count} <- drops do
      :telemetry.execute(
        [:logflare, :backends, :victoria_metrics, :drop],
        %{count: count},
        %{reason: reason, backend_id: backend_id}
      )
    end

    :ok
  end

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
