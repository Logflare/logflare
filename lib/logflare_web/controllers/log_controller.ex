defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  alias Logflare.Logs
  alias Logflare.Logs.Processor
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceRequest
  alias Opentelemetry.Proto.Collector.Trace.V1.ExportTraceServiceResponse
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceRequest
  alias Opentelemetry.Proto.Collector.Logs.V1.ExportLogsServiceResponse

  alias LogflareWeb.OpenApi.Created
  alias LogflareWeb.OpenApi.ServerError
  alias LogflareWeb.OpenApiSchemas.LogsCreated

  action_fallback(LogflareWeb.Api.FallbackController)

  tags(["Public"])

  plug(
    CORSPlug,
    [
      origin: "*",
      max_age: 1_728_000,
      headers: [
        "Authorization",
        "Content-Type",
        "Content-Length",
        "X-Requested-With"
      ],
      methods: ["POST", "OPTIONS"],
      send_preflight_response?: true
    ]
    when action in [:browser_reports, :generic_json, :create]
  )

  plug(LogflareWeb.Plugs.BlockSystemSource)

  @message "Logged!"

  operation(:create,
    summary: "Create log event",
    description:
      "Full details are available in the [ingestion documentation](https://docs.logflare.app/concepts/ingestion/)",
    parameters: [
      source: [
        in: :query,
        description: "Source UUID",
        type: :string,
        example: "a040ae88-3e27-448b-9ee6-622278b23193",
        required: false
      ],
      source_name: [
        in: :query,
        description: "Source name",
        type: :string,
        example: "MyApp.MySource",
        required: false
      ]
    ],
    responses: %{
      200 => Created.response(LogsCreated),
      500 => ServerError.response()
    }
  )

  def create(conn, %{"batch" => batch}) when is_list(batch) do
    create_dispatch(conn, batch)
  end

  def create(conn, %{"_json" => batch}) when is_list(batch) do
    create_dispatch(conn, batch)
  end

  def create(conn, _slog_params) do
    event = Map.drop(conn.body_params, ~w[timestamp id])
    create_dispatch(conn, [event])
  end

  @lf_source_key "__LF_SOURCE"

  defp create_dispatch(conn, events) when is_list(events) do
    default_source = Map.get(conn.assigns, :source)
    multi_source? = Map.has_key?(conn.assigns, :declared_sources)

    if not multi_source? and default_source != nil do
      events
      |> Processor.ingest(Logs.Raw, default_source)
      |> handle(conn)
    else
      declared = Map.get(conn.assigns, :declared_sources, %{})
      {grouped, errors} = group_events_by_source(events, declared, default_source)

      results =
        for {source, batch} <- grouped do
          Processor.ingest(batch, Logs.Raw, source)
        end

      results
      |> aggregate_results(errors)
      |> handle(conn)
    end
  end

  defp group_events_by_source(events, declared, default_source) do
    {groups, errors} =
      for event <- events, reduce: {%{}, []} do
        {groups, errors} ->
          token = event_source_token(event)
          stripped = Map.drop(event, [@lf_source_key, :"#{@lf_source_key}"])

          cond do
            is_binary(token) and is_map_key(declared, token) ->
              source = Map.fetch!(declared, token)
              {Map.update(groups, source, [stripped], &[stripped | &1]), errors}

            is_binary(token) ->
              {groups, ["event references unknown or unauthorized source #{token}" | errors]}

            token != nil ->
              {groups, ["invalid __LF_SOURCE value" | errors]}

            default_source != nil ->
              {Map.update(groups, default_source, [stripped], &[stripped | &1]), errors}

            true ->
              {groups, ["event missing __LF_SOURCE and no source query param" | errors]}
          end
      end

    grouped = for {source, batch} <- groups, do: {source, Enum.reverse(batch)}
    {grouped, Enum.reverse(errors)}
  end

  defp event_source_token(%{@lf_source_key => token}), do: token
  defp event_source_token(%{:"__LF_SOURCE" => token}), do: token
  defp event_source_token(_), do: nil

  defp aggregate_results(results, errors) do
    {count, all_errors} =
      for result <- results, reduce: {0, errors} do
        {acc, errs} ->
          case result do
            {:ok, n} -> {acc + n, errs}
            :ok -> {acc, errs}
            {:error, more} when is_list(more) -> {acc, errs ++ more}
            {:error, err} -> {acc, errs ++ [err]}
          end
      end

    if all_errors == [], do: {:ok, count}, else: {:error, all_errors}
  end

  operation :cloudflare, false

  def cloudflare(%{assigns: %{source: source}} = conn, %{"batch" => batch}) when is_list(batch) do
    batch
    |> Processor.ingest(Logs.Raw, source)
    |> handle(conn)
  end

  def cloudflare(%{assigns: %{source: source}} = conn, log_params) when is_map(log_params) do
    log_params
    |> Map.drop(["source", "timestamp", "id"])
    |> List.wrap()
    |> Processor.ingest(Logs.Raw, source)
    |> handle(conn)
  end

  operation :syslog, false

  def syslog(%{assigns: %{source: source}} = conn, %{"batch" => batch}) when is_list(batch) do
    batch
    |> Processor.ingest(Logs.Raw, source)
    |> handle(conn)
  end

  operation :generic_json, false

  def generic_json(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch
    |> Processor.ingest(Logs.GenericJson, source)
    |> handle(conn)
  end

  def generic_json(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    event
    |> List.wrap()
    |> Processor.ingest(Logs.GenericJson, source)
    |> handle(conn)
  end

  operation :vector, false

  def vector(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch
    |> Processor.ingest(Logs.Vector, source)
    |> handle(conn)
  end

  def vector(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    event
    |> List.wrap()
    |> Processor.ingest(Logs.Vector, source)
    |> handle(conn)
  end

  operation :browser_reports, false

  def browser_reports(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch
    |> Processor.ingest(Logs.BrowserReport, source)
    |> handle(conn)
  end

  def browser_reports(%{assigns: %{source: source}, body_params: event} = conn, _log_params) do
    event
    |> List.wrap()
    |> Processor.ingest(Logs.BrowserReport, source)
    |> handle(conn)
  end

  operation :elixir_logger, false

  def elixir_logger(%{assigns: %{source: source}} = conn, %{"batch" => batch})
      when is_list(batch) do
    batch
    |> Processor.ingest(Logs.Raw, source)
    |> handle(conn)
  end

  operation :create_with_typecasts, false

  def create_with_typecasts(%{assigns: %{source: source}} = conn, %{"batch" => batch})
      when is_list(batch) do
    batch
    |> Processor.ingest(Logs.IngestTypecasting, source)
    |> handle(conn)
  end

  operation :vercel_ingest, false

  def vercel_ingest(%{assigns: %{source: source}} = conn, %{"_json" => batch})
      when is_list(batch) do
    batch
    |> Processor.ingest(Logs.Vercel, source)
    |> handle(conn)
  end

  operation :netlify, false

  def netlify(%{assigns: %{source: source}} = conn, %{"_json" => batch}) when is_list(batch) do
    batch
    |> Processor.ingest(Logs.Netlify, source)
    |> handle(conn)
  end

  def netlify(%{assigns: %{source: source}, body_params: params} = conn, _params)
      when is_map(params) do
    [params]
    |> Processor.ingest(Logs.Netlify, source)
    |> handle(conn)
  end

  operation :github, false

  def github(%{assigns: %{source: source}, body_params: params} = conn, _params) do
    [params]
    |> Processor.ingest(Logs.Github, source)
    |> handle(conn)
  end

  operation :cloud_event, false

  def cloud_event(%Plug.Conn{} = conn, %{"_json" => batch})
      when is_list(batch) do
    do_cloud_event(conn, batch)
  end

  def cloud_event(%Plug.Conn{body_params: event} = conn, _log_params) do
    do_cloud_event(conn, event)
  end

  defp do_cloud_event(%Plug.Conn{assigns: %{source: source}} = conn, data) do
    cloud_event = extract_cloud_events(conn)

    timestamp = cloud_event["time"]

    data
    |> List.wrap()
    |> Enum.map(
      &Map.merge(&1, %{
        "timestamp" => timestamp,
        "cloud_event" => cloud_event
      })
    )
    |> Processor.ingest(Logs.Raw, source)
    |> handle(conn)
  end

  defp handle({:ok, _}, conn), do: render(conn, "index.json", message: @message)
  defp handle(:ok, conn), do: render(conn, "index.json", message: @message)

  defp handle({:error, errors}, conn) do
    conn
    |> put_status(406)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: errors)
  end

  defp extract_cloud_events(%Plug.Conn{} = conn) do
    # XXX: What should happen in case of duplicated header?
    for {"ce-" <> header, data} <- conn.req_headers, into: %{} do
      {String.replace(header, "-", "_"), data}
    end
  end

  operation :otel_traces, false

  def otel_traces(
        %{assigns: %{source: source}} = conn,
        %ExportTraceServiceRequest{resource_spans: resource_spans}
      ) do
    resource_spans
    |> Processor.ingest(Logs.OtelTrace, source)
    |> protobuf_response(conn, %ExportTraceServiceResponse{})
  rescue
    exception ->
      send_proto_error(conn, 500, "Internal server error")
      reraise exception, __STACKTRACE__
  end

  operation :otel_metrics, false

  def otel_metrics(
        %{assigns: %{source: source}} = conn,
        %ExportMetricsServiceRequest{resource_metrics: resource_metrics}
      ) do
    resource_metrics
    |> Processor.ingest(Logs.OtelMetric, source)
    |> protobuf_response(conn, %ExportMetricsServiceResponse{})
  rescue
    exception ->
      send_proto_error(conn, 500, "Internal server error")
      reraise exception, __STACKTRACE__
  end

  operation :otel_logs, false

  def otel_logs(
        %{assigns: %{source: source}} = conn,
        %ExportLogsServiceRequest{resource_logs: resource_logs}
      ) do
    resource_logs
    |> Processor.ingest(Logs.OtelLog, source)
    |> protobuf_response(conn, %ExportLogsServiceResponse{})
  rescue
    exception ->
      send_proto_error(conn, 500, "Internal server error")
      reraise exception, __STACKTRACE__
  end

  defp protobuf_response({:error, _}, conn, _success_response) do
    send_proto_error(conn, 500, "Internal server error")
  end

  defp protobuf_response(_, conn, success_response) do
    send_proto_resp(conn, success_response)
  end

  defp send_proto_resp(conn, resp) do
    payload = Protobuf.encode_to_iodata(resp)

    conn
    |> put_resp_content_type("application/x-protobuf")
    |> send_resp(200, payload)
  end

  defp send_proto_error(conn, status, error) do
    conn
    |> send_resp(status, Protobuf.encode(%Google.Rpc.Status{message: error}))
    |> halt()
  end
end
