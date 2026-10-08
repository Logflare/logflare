defmodule Logflare.Backends.Adaptor.VictoriaMetricsAdaptor do
  @moduledoc """
  Backend adaptor for VictoriaMetrics using Prometheus remote write protocol (v0.1.0).

  The payload is protobuf-encoded and snappy block-compressed before POST. The URL
  should point to the VictoriaMetrics remote-write endpoint, e.g.
  https://victoriametrics.example.com/api/v1/write.

  Only metric events (`LogEvent.event_type == :metric`) are sent. Gauges, cumulative
  sums and cumulative explicit-bucket histograms are supported. Everything else is
  dropped and counted in a `[:logflare, :backends, :victoria_metrics, :drop]`
  telemetry event per reason; a batch that drops metric events also logs a warning.

    * `:not_a_metric` - log and trace events
    * `:unsupported_type` - exponential histograms, summaries and unknown types
    * `:non_cumulative` - sums and histograms with delta or unspecified
      temporality, which remote write cannot represent
    * `:invalid` - malformed data points, such as a missing name or value, or
      bucket counts that do not match the bounds

  Labels on each series:

    * `source` - the source name
    * `job` and `instance` - from the `service.namespace`/`service.name` and
      `service.instance.id` resource attributes, when present
    * data point attributes with non-empty string, number or boolean values; list
      and map values are skipped. Names are as stored by Logflare, which normalizes
      keys at ingest (e.g. `http.route` becomes `_http_route`)
    * the optional `labels` config map, which wins over attributes on collision

  Labels the adaptor sets (`__name__`, `le`, `source`, `job`, `instance`) always win.
  An attribute or config label with one of those names is kept as `exported_<name>`.
  """

  @behaviour Logflare.Backends.Adaptor

  require Logger

  alias Logflare.Backends.Adaptor.HttpBased.Headers
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.Query
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.RemoteWrite
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.LogEvent
  alias Logflare.Sources
  alias Logflare.Sources.Source
  alias Logflare.Utils

  @reserved_labels ["__name__", "le", "source", "job", "instance"]
  @redacted_value Headers.redacted_value()
  @max_float 1.7_976_931_348_623_157e308
  # Snappy encoding of an empty WriteRequest. snappyer returns "" for empty input,
  # which strict snappy decoders reject.
  @empty_write_request <<0>>

  @type drop_reason :: :not_a_metric | :unsupported_type | :non_cumulative | :invalid

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
    changeset =
      {existing_config,
       %{
         url: :string,
         query_url: :string,
         headers: :map,
         username: :string,
         password: :string,
         labels: :map
       }}
      |> Ecto.Changeset.cast(params, [:url, :query_url, :headers, :username, :password, :labels])

    cond do
      destination_changed?(changeset, existing_config) ->
        changeset
        |> WebhookAdaptor.unredact_credentials(Map.drop(existing_config, [:headers, "headers"]))
        |> require_new_credentials(existing_config)

      query_destination_changed?(changeset, existing_config) ->
        changeset
        |> WebhookAdaptor.unredact_credentials(existing_config)
        |> require_new_password()

      true ->
        changeset
        |> WebhookAdaptor.unredact_credentials(existing_config)
        |> unredact_password()
    end
  end

  @impl Logflare.Backends.Adaptor
  @spec validate_config(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  def validate_config(changeset) do
    changeset
    |> Ecto.Changeset.validate_required([:url])
    |> Ecto.Changeset.validate_format(:url, ~r/https?\:\/\/.+/)
    |> Ecto.Changeset.validate_change(:query_url, &validate_query_url/2)
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
    WebhookAdaptor.test_connection(backend, @empty_write_request)
  end

  @spec execute_promql(Backend.t(), String.t(), map()) ::
          {:ok, map()} | {:error, {pos_integer(), map()}}
  defdelegate execute_promql(backend, query, params), to: Query, as: :execute

  @spec validate_query_url(:query_url, String.t()) :: keyword(String.t())
  defp validate_query_url(:query_url, url) do
    case Query.validate_url(url) do
      :ok -> []
      {:error, message} -> [query_url: message]
    end
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

  # Stored credentials only ever go to the destination they were entered for. When the
  # URL moves to another origin, the password and credential headers must be entered
  # again rather than restored from the redacted form or kept from storage.
  defp require_new_credentials(changeset, existing_config) do
    changeset = require_new_password(changeset)

    case Ecto.Changeset.get_change(changeset, :headers) do
      nil ->
        stored = Map.get(existing_config, :headers) || Map.get(existing_config, "headers") || %{}

        kept =
          for {key, value} <- stored, not Headers.sensitive?(key), into: %{}, do: {key, value}

        Ecto.Changeset.put_change(changeset, :headers, kept)

      _submitted ->
        changeset
    end
  end

  @spec require_new_password(Ecto.Changeset.t()) :: Ecto.Changeset.t()
  defp require_new_password(changeset) do
    case changeset.params["password"] do
      password when is_binary(password) and password not in ["", @redacted_value] ->
        Ecto.Changeset.put_change(changeset, :password, password)

      _ ->
        Ecto.Changeset.put_change(changeset, :password, nil)
    end
  end

  @spec query_destination_changed?(Ecto.Changeset.t(), map()) :: boolean()
  defp query_destination_changed?(changeset, existing_config) do
    previous_url =
      existing_config[:query_url] || existing_config["query_url"] ||
        existing_config[:url] || existing_config["url"]

    case Ecto.Changeset.get_change(changeset, :query_url) do
      url when is_binary(url) and is_binary(previous_url) -> origin(url) != origin(previous_url)
      _ -> false
    end
  end

  defp destination_changed?(changeset, existing_config) do
    existing_url = Map.get(existing_config, :url) || Map.get(existing_config, "url")

    case Ecto.Changeset.get_change(changeset, :url) do
      url when is_binary(url) and is_binary(existing_url) -> origin(url) != origin(existing_url)
      _ -> false
    end
  end

  @spec origin(String.t()) ::
          {String.t() | nil, String.t() | nil, non_neg_integer() | nil} | :invalid
  defp origin(url) do
    case URI.new(url) do
      {:ok, uri} -> {uri.scheme, uri.host && String.downcase(uri.host), uri.port}
      {:error, _reason} -> :invalid
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

  defp labeled_samples(%{event_type: :metric, body: body} = event, context) do
    with {:ok, kind} <- series_kind(body),
         {:ok, name} <- metric_name(body["event_message"]),
         {:ok, point} <- data_point(kind, body) do
      labels = series_labels(body, Map.get(context.source_names, event.source_id), context)
      {:ok, samples(point, name, labels, micro_to_ms(body["timestamp"]))}
    end
  end

  defp labeled_samples(_event, _context), do: {:drop, :not_a_metric}

  defp series_kind(%{"metric_type" => "gauge"}), do: {:ok, :value}
  defp series_kind(%{"metric_type" => "sum"} = body), do: cumulative_kind(body, :value)
  defp series_kind(%{"metric_type" => "histogram"} = body), do: cumulative_kind(body, :histogram)
  defp series_kind(_body), do: {:drop, :unsupported_type}

  # Events without a temporality did not come from OTLP and are taken as cumulative.
  defp cumulative_kind(%{"aggregation_temporality" => temporality}, _kind)
       when temporality not in [nil, "cumulative"],
       do: {:drop, :non_cumulative}

  defp cumulative_kind(_body, kind), do: {:ok, kind}

  defp metric_name(name) when is_binary(name) and name != "", do: {:ok, sanitize_name(name, true)}
  defp metric_name(_name), do: {:drop, :invalid}

  defp data_point(:value, body) do
    case sample_value(body["value"]) do
      {:ok, value} -> {:ok, {:value, value}}
      :error -> {:drop, :invalid}
    end
  end

  # Ingest strips empty lists and nils, so absent buckets or sum arrive as missing keys.
  defp data_point(:histogram, body) do
    counts = Map.get(body, "bucket_counts", [])
    bounds = Map.get(body, "explicit_bounds", [])

    with true <- valid_buckets?(counts, bounds),
         count = Map.get(body, "count", Enum.sum(counts)),
         true <- is_integer(count) and count >= 0,
         {:ok, sum} <- optional_sample_value(body["sum"]) do
      {:ok, {:histogram, count, sum, cumulative_buckets(counts, bounds)}}
    else
      _ -> {:drop, :invalid}
    end
  end

  # OTLP requires one more count than bounds (the overflow bucket), or no buckets.
  defp valid_buckets?(counts, bounds) when is_list(counts) and is_list(bounds) do
    (counts == [] or length(counts) == length(bounds) + 1) and
      Enum.all?(counts, &(is_integer(&1) and &1 >= 0)) and
      strictly_increasing?(bounds)
  end

  defp valid_buckets?(_counts, _bounds), do: false

  defp strictly_increasing?([a, b | rest]) when is_number(a) and is_number(b) and a < b,
    do: strictly_increasing?([b | rest])

  defp strictly_increasing?([a]) when is_number(a), do: true
  defp strictly_increasing?([]), do: true
  defp strictly_increasing?(_bounds), do: false

  # The overflow count is not needed: the +Inf bucket is always `count`, as in the
  # Prometheus OTLP translator.
  defp cumulative_buckets([], _bounds), do: []

  defp cumulative_buckets(counts, bounds) do
    {buckets, _cumulative} =
      counts
      |> Enum.zip(bounds)
      |> Enum.map_reduce(0, fn {count, bound}, cumulative ->
        cumulative = cumulative + count
        {{format_float(bound), cumulative}, cumulative}
      end)

    buckets
  end

  # `labels` is sorted by name and never holds `__name__` or `le`, so inserting them
  # keeps every series' label list in the order remote write requires.
  defp samples({:value, value}, name, labels, ts_ms) do
    [{insert_label(labels, "__name__", name), {value, ts_ms}}]
  end

  defp samples({:histogram, count, sum, buckets}, name, labels, ts_ms) do
    bucket_labels = insert_label(labels, "__name__", name <> "_bucket")
    count_value = integer_value(count)

    bucket_samples =
      for {le, cumulative} <- buckets do
        {insert_label(bucket_labels, "le", le), {integer_value(cumulative), ts_ms}}
      end

    sum_samples =
      if sum == nil,
        do: [],
        else: [{insert_label(labels, "__name__", name <> "_sum"), {sum, ts_ms}}]

    [
      {insert_label(labels, "__name__", name <> "_count"), {count_value, ts_ms}},
      {insert_label(bucket_labels, "le", "+Inf"), {count_value, ts_ms}}
      | sum_samples ++ bucket_samples
    ]
  end

  defp insert_label([{key, _value} = label | rest], name, value) when key < name,
    do: [label | insert_label(rest, name, value)]

  defp insert_label(labels, name, value), do: [{name, value} | labels]

  # protobuf decodes IEEE special values to atoms, which RemoteWrite encodes back.
  defp sample_value(value) when is_float(value), do: {:ok, value}
  defp sample_value(value) when is_integer(value), do: {:ok, integer_value(value)}
  defp sample_value(value) when value in [:nan, :infinity, :negative_infinity], do: {:ok, value}
  defp sample_value(_value), do: :error

  defp optional_sample_value(nil), do: {:ok, nil}
  defp optional_sample_value(value), do: sample_value(value)

  defp integer_value(value) when abs(value) <= @max_float, do: value * 1.0
  defp integer_value(value) when value > 0, do: :infinity
  defp integer_value(_value), do: :negative_infinity

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

  # Repeats the prefix when `exported_<name>` is itself taken, as Prometheus does.
  defp export_reserved(labels) do
    Enum.reduce(@reserved_labels, labels, fn name, acc ->
      case Map.pop(acc, name) do
        {nil, acc} -> acc
        {value, acc} -> Map.put(acc, exported_name(acc, "exported_" <> name), value)
      end
    end)
  end

  defp exported_name(labels, name) do
    if Map.has_key?(labels, name), do: exported_name(labels, "exported_" <> name), else: name
  end

  # Prometheus treats an empty label value as an absent label, and an empty name is
  # invalid, so both are skipped.
  defp flat_labels(labels) when is_map(labels) do
    Enum.reduce(labels, %{}, fn {key, value}, acc ->
      with true <- is_binary(key) or is_atom(key),
           {:ok, value} <- label_value(value),
           name when name != "" <- sanitize_name(to_string(key), false) do
        Map.put(acc, name, value)
      else
        _ -> acc
      end
    end)
  end

  defp flat_labels(_labels), do: %{}

  defp label_value(value) when is_binary(value) and value != "", do: {:ok, value}
  defp label_value(value) when is_float(value), do: {:ok, format_float(value)}
  defp label_value(value) when is_integer(value) or is_boolean(value), do: {:ok, to_string(value)}
  defp label_value(_value), do: :error

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

  # LogEvent.make/2 normalizes body["timestamp"] to microseconds.
  defp micro_to_ms(us) when is_integer(us), do: div(us, 1_000)
  defp micro_to_ms(_), do: System.system_time(:millisecond)

  @spec report_drops(%{drop_reason() => pos_integer()}, non_neg_integer(), term()) :: :ok
  defp report_drops(drops, _total, _backend_id) when map_size(drops) == 0, do: :ok

  defp report_drops(drops, total, backend_id) do
    for {reason, count} <- drops do
      :telemetry.execute(
        [:logflare, :backends, :victoria_metrics, :drop],
        %{count: count},
        %{reason: reason, backend_id: backend_id}
      )
    end

    # Log and trace events routed here are expected noise; only metric drops warn.
    metric_drops = Map.delete(drops, :not_a_metric)

    if map_size(metric_drops) > 0 do
      dropped = Enum.reduce(metric_drops, 0, fn {_reason, count}, acc -> acc + count end)

      Logger.warning(
        "Dropping #{dropped} of #{total} VictoriaMetrics metric event(s): #{inspect(metric_drops)}",
        backend_id: backend_id
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
