defmodule Logflare.Backends.Adaptor.OtlpAdaptor.ProtobufFormatter do
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.KeyValueList
  alias Opentelemetry.Proto.Logs.V1.LogRecord
  alias Opentelemetry.Proto.Logs.V1.ResourceLogs
  alias Opentelemetry.Proto.Logs.V1.ScopeLogs
  alias Opentelemetry.Proto.Logs.V1.SeverityNumber
  alias Opentelemetry.Proto.Resource.V1.Resource
  alias Logflare.LogEvent

  @behaviour Tesla.Middleware

  @protobuf_content_type "application/x-protobuf"

  @impl true
  def call(env, next, opts) do
    source = env.opts[:source] || %{}
    project_ref = env.opts |> Keyword.get(:metadata, %{}) |> Map.get("backend.project_ref")
    flatten_to_attributes? = opts[:flatten_to_attributes] || false

    response =
      env
      |> put_content_type_header()
      |> Tesla.put_body(transform_batch(env.body, source, project_ref, flatten_to_attributes?))
      |> Tesla.run(next)

    with {:ok, %{status: 200} = env} <- response do
      if Tesla.get_header(env, "content-type") == @protobuf_content_type do
        response = Protobuf.decode(env.body, ExportLogsServiceResponse)
        {:ok, Tesla.put_body(env, response)}
      else
        response
      end
    end
  end

  # Strips any pre-existing content-type header case-insensitively before setting ours,
  # since Tesla.put_header/3 matches keys case-sensitively and would otherwise leave a
  # user-configured "Content-Type" header alongside this one, sending two content-type
  # header lines that some receivers (e.g. Envoy-fronted ingests) reject outright.
  defp put_content_type_header(env) do
    headers = Enum.reject(env.headers, fn {k, _v} -> String.downcase(k) == "content-type" end)
    Tesla.put_header(%{env | headers: headers}, "content-type", @protobuf_content_type)
  end

  @doc """
  Header names this formatter sets, so `HttpBased.Client` can drop any
  user-supplied copies and keep itself the single source (see
  `Logflare.Backends.Adaptor.HttpBased.Headers.drop_reserved/2`).
  """
  @spec reserved_headers() :: [String.t()]
  def reserved_headers, do: ["content-type"]

  defp transform_batch(events, source, project_ref, flatten_to_attributes?) do
    %ExportLogsServiceRequest{
      resource_logs: [
        %ResourceLogs{
          resource: build_resource(source, project_ref),
          scope_logs: build_scope_logs(events, flatten_to_attributes?),
          schema_url: "https://opentelemetry.io/schemas/1.26.0"
        }
      ]
    }
    |> Protobuf.encode_to_iodata()
  end

  # Resource identifies the entity the telemetry is *about* — for a hosted log
  # drain that's the customer's own project/component, not Logflare itself, so
  # this must come from the source/backend rather than being a hardcoded constant.
  defp build_resource(source, project_ref) do
    attributes =
      case resource_service_name(source) do
        {:logflare, name} ->
          [
            "service.name": name,
            "service.version": Application.spec(:logflare, :vsn) |> to_string()
          ]

        {:source, name} ->
          # We have no reliable version for the customer's own service, so
          # service.version is omitted rather than reporting Logflare's version
          # under a name that no longer refers to Logflare.
          ["service.name": name]
      end
      |> maybe_add_namespace(project_ref)
      |> Enum.map(&make_key_value/1)

    %Resource{attributes: attributes}
  end

  # service.namespace groups service.name under the customer's project, since the
  # same source (e.g. "postgres") is shared across every customer's log drain and
  # can't identify a project on its own. Omitted when unavailable (non-log-drain
  # OTLP backends don't carry a project_ref in their metadata).
  defp maybe_add_namespace(attrs, project_ref) when is_binary(project_ref) and project_ref != "",
    do: attrs ++ ["service.namespace": project_ref]

  defp maybe_add_namespace(attrs, _project_ref), do: attrs

  # `source` may be a real %Source{} struct, which doesn't implement Access, or
  # the empty map fallback used when no source is available (e.g. test_connection/1).
  defp resource_service_name(%{service_name: service_name}) when is_binary(service_name),
    do: {:source, service_name}

  defp resource_service_name(%{name: name}) when is_binary(name), do: {:source, name}
  defp resource_service_name(_source), do: {:logflare, "Logflare"}

  # Scope identifies the instrumentation library producing the telemetry (i.e.
  # Logflare's own exporter), which is correctly Logflare regardless of source —
  # unlike Resource, this is not customer-specific.
  defp build_scope_logs(logs, flatten_to_attributes?) do
    [
      %ScopeLogs{
        scope: %InstrumentationScope{
          name: "Logflare",
          version: Application.spec(:logflare, :vsn) |> to_string()
        },
        log_records: Enum.map(logs, &build_log_record(&1, flatten_to_attributes?))
      }
    ]
  end

  defp build_log_record(%LogEvent{} = ev, flatten_to_attributes?) do
    observed_ts =
      if ev.ingested_at, do: DateTime.to_unix(ev.ingested_at, :nanosecond), else: 0

    {known_entries, extra} =
      Map.split(ev.body, [
        "timestamp",
        "event_message",
        "attributes",
        "trace_id",
        "span_id",
        "severity_number",
        "severity_text"
      ])

    fields =
      known_entries
      |> Enum.flat_map(&build_log_record_fields(&1, flatten_to_attributes?))
      |> maybe_derive_severity(ev.body)

    {body, fields} = build_body_and_attributes(flatten_to_attributes?, ev.body, extra, fields)

    struct!(LogRecord, [observed_time_unix_nano: observed_ts, body: body] ++ fields)
  end

  # Opt-in (backend config `flatten_to_attributes: true`, defaults to false): body
  # becomes the human-readable message, and everything else the source sends
  # (metadata, ids, etc.) is flattened into attributes instead. AnyValue permits
  # a structured body per the OTel spec, but attributes is the field most
  # backends actually build search/filter UI against — Dash0 confirmed
  # stringifying non-scalar attribute values, though we haven't verified it does
  # the same to a structured body specifically. Defaults to the legacy behavior
  # (everything in body, unflattened) so existing backends are unaffected.
  defp build_body_and_attributes(true, raw_body, extra, fields) do
    {log_body(raw_body), add_extra_attributes(fields, extra)}
  end

  defp build_body_and_attributes(false, _raw_body, extra, fields) do
    {make_value(extra), fields}
  end

  defp log_body(%{"event_message" => msg}) when is_binary(msg), do: make_value(msg)
  defp log_body(_body), do: make_value("")

  # Merges the source's leftover fields into attributes rather than overwriting
  # any explicit "attributes" the source already provided via build_log_record_fields.
  defp add_extra_attributes(fields, extra) when extra == %{}, do: fields

  defp add_extra_attributes(fields, extra) do
    extra_attributes = flatten_attributes(extra)
    Keyword.update(fields, :attributes, extra_attributes, &(&1 ++ extra_attributes))
  end

  # Recurses into nested maps, turning each leaf into its own dotted top-level
  # attribute (e.g. "metadata.response.status_code") instead of one attribute
  # whose value is itself a nested kvlist_value. Receivers commonly only index
  # flat scalar attribute values, storing anything else (a whole sub-object) as
  # an opaque JSON string instead — flattening keeps every field queryable.
  defp flatten_attributes(map, prefix \\ nil) do
    Enum.flat_map(map, fn {k, v} ->
      key = if prefix, do: "#{prefix}.#{k}", else: to_string(k)

      case v do
        v when is_map(v) -> flatten_attributes(v, key)
        v -> [make_key_value({key, v})]
      end
    end)
  end

  # None of the Supabase log sources set an explicit "severity_number", but most
  # carry an equivalent signal elsewhere in their metadata — deriving from that
  # rather than leaving every log at the zero-value UNSPECIFIED severity.
  defp maybe_derive_severity(fields, body) do
    if Keyword.has_key?(fields, :severity_number) do
      fields
    else
      case derive_severity_number(body) do
        nil -> fields
        severity -> Keyword.put(fields, :severity_number, severity)
      end
    end
  end

  # Postgres's own error_severity takes priority over the generic signals below
  # when present, since it's the source reporting its severity directly rather
  # than something inferred from an unrelated field (level string, HTTP status).
  defp derive_severity_number(%{"metadata" => %{"parsed" => %{"error_severity" => level}}})
       when is_binary(level),
       do: severity_from_postgres_level(level)

  defp derive_severity_number(%{"metadata" => %{"level" => level}}) when is_binary(level),
    do: severity_from_level(level)

  defp derive_severity_number(%{"metadata" => %{"response" => %{"status_code" => status}}})
       when is_integer(status),
       do: severity_from_status_code(status)

  defp derive_severity_number(_body), do: nil

  defp severity_from_postgres_level(level) do
    case String.upcase(level) do
      "PANIC" -> :SEVERITY_NUMBER_FATAL
      "FATAL" -> :SEVERITY_NUMBER_FATAL
      "ERROR" -> :SEVERITY_NUMBER_ERROR
      "WARNING" -> :SEVERITY_NUMBER_WARN
      "NOTICE" -> :SEVERITY_NUMBER_INFO
      "LOG" -> :SEVERITY_NUMBER_INFO
      "INFO" -> :SEVERITY_NUMBER_INFO
      "DEBUG1" -> :SEVERITY_NUMBER_DEBUG
      "DEBUG2" -> :SEVERITY_NUMBER_DEBUG
      "DEBUG3" -> :SEVERITY_NUMBER_DEBUG
      "DEBUG4" -> :SEVERITY_NUMBER_DEBUG
      "DEBUG5" -> :SEVERITY_NUMBER_DEBUG
      _unrecognized -> nil
    end
  end

  defp severity_from_level(level) do
    case String.downcase(level) do
      "trace" -> :SEVERITY_NUMBER_TRACE
      "debug" -> :SEVERITY_NUMBER_DEBUG
      "info" -> :SEVERITY_NUMBER_INFO
      "notice" -> :SEVERITY_NUMBER_INFO
      "warn" -> :SEVERITY_NUMBER_WARN
      "warning" -> :SEVERITY_NUMBER_WARN
      "error" -> :SEVERITY_NUMBER_ERROR
      "critical" -> :SEVERITY_NUMBER_FATAL
      "fatal" -> :SEVERITY_NUMBER_FATAL
      "panic" -> :SEVERITY_NUMBER_FATAL
      _unrecognized -> nil
    end
  end

  defp severity_from_status_code(status) when status >= 500, do: :SEVERITY_NUMBER_ERROR
  defp severity_from_status_code(status) when status >= 400, do: :SEVERITY_NUMBER_WARN
  defp severity_from_status_code(_status), do: :SEVERITY_NUMBER_INFO

  defp build_log_record_fields({"timestamp", ts}, _flatten_to_attributes?),
    do: [time_unix_nano: System.convert_time_unit(ts, :microsecond, :nanosecond)]

  # In legacy (non-structured) mode, event_name carries the message, same as before
  # the body/attributes split existed.
  defp build_log_record_fields({"event_message", msg}, false) when is_binary(msg),
    do: [event_name: msg]

  # In structured mode, event_message drives body (see log_body/1) instead —
  # event_name is meant to be a low-cardinality event-type identifier, and the
  # full message is the opposite of that.
  defp build_log_record_fields({"event_message", _msg}, true), do: []

  defp build_log_record_fields({"attributes", attrs}, true) when is_map(attrs),
    do: [attributes: flatten_attributes(attrs)]

  defp build_log_record_fields({"attributes", attrs}, false) when is_map(attrs),
    do: [attributes: Enum.map(attrs, &make_key_value/1)]

  defp build_log_record_fields({"severity_number", number}, _flatten_to_attributes?)
       when is_integer(number),
       do: [severity_number: SeverityNumber.key(number)]

  defp build_log_record_fields({"severity_text", msg}, _flatten_to_attributes?)
       when is_binary(msg),
       do: [severity_text: msg]

  defp build_log_record_fields({"trace_id", id}, _flatten_to_attributes?) when is_binary(id),
    do: build_id_field(:trace_id, id)

  defp build_log_record_fields({"span_id", id}, _flatten_to_attributes?) when is_binary(id),
    do: build_id_field(:span_id, id)

  defp build_log_record_fields(_unmatched, _flatten_to_attributes?), do: []

  # trace_id/span_id arrive as hex-encoded strings (the standard OTel textual
  # representation); the protobuf fields require raw bytes, so this must decode
  # them first or the receiver sees a hex-length-instead-of-byte-length value.
  # Falls back to sending the raw value as-is if it isn't valid hex, matching
  # prior behavior rather than dropping the field outright.
  defp build_id_field(field, hex_id) do
    case Base.decode16(hex_id, case: :mixed) do
      {:ok, decoded} -> [{field, decoded}]
      :error -> [{field, hex_id}]
    end
  end

  defp make_value(v) when is_binary(v), do: %AnyValue{value: {:string_value, v}}
  defp make_value(v) when is_boolean(v), do: %AnyValue{value: {:bool_value, v}}
  defp make_value(v) when is_integer(v), do: %AnyValue{value: {:int_value, v}}
  defp make_value(v) when is_float(v), do: %AnyValue{value: {:double_value, v}}
  defp make_value(v) when is_list(v), do: %AnyValue{value: {:array_value, make_array(v)}}
  defp make_value(v) when is_map(v), do: %AnyValue{value: {:kvlist_value, make_key_value_list(v)}}
  # TODO: distinguish from string
  defp make_value(v) when is_binary(v), do: %AnyValue{value: {:bytes_value, v}}

  defp make_key_value({k, v}), do: %KeyValue{key: to_string(k), value: make_value(v)}

  defp make_key_value_list(kv) do
    %KeyValueList{values: Enum.map(kv, &make_key_value/1)}
  end

  defp make_array(enum) do
    %ArrayValue{values: Enum.map(enum, &make_value/1)}
  end
end
