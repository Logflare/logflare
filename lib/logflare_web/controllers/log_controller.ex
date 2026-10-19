defmodule LogflareWeb.LogController do
  use LogflareWeb, :controller
  use OpenApiSpex.ControllerSpecs

  require Logger

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
  @source_unavailable_message "Source is unavailable. Send the batch again."
  @source_unavailable_retry_after "5"
  @source_not_found_message "Source not found."

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
    dispatch(batch, conn, Logs.Raw)
  end

  def create(conn, %{"_json" => batch}) when is_list(batch) do
    dispatch(batch, conn, Logs.Raw)
  end

  def create(conn, _slog_params) do
    event = Map.drop(conn.body_params, ~w[timestamp id])
    dispatch([event], conn, Logs.Raw)
  end

  @lf_source_key "__LF_SOURCE"

  defp dispatch(events, conn, processor) when is_list(events) do
    default_source = Map.get(conn.assigns, :source)
    multi_source? = Map.has_key?(conn.assigns, :declared_sources)

    if not multi_source? and default_source != nil do
      events
      |> Enum.map(&drop_source_key/1)
      |> Processor.ingest(processor, default_source)
      |> handle(conn)
    else
      declared = Map.get(conn.assigns, :declared_sources, %{})
      {grouped, errors} = group_events_by_source(events, declared, default_source)

      results =
        for {source, batch} <- grouped do
          Processor.ingest(batch, processor, source)
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
          stripped = drop_source_key(event)

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

  defp drop_source_key(event) when is_map(event),
    do: Map.drop(event, [@lf_source_key, :"#{@lf_source_key}"])

  defp drop_source_key(event), do: event

  defp event_source_token(%{@lf_source_key => token}), do: token
  defp event_source_token(%{:__LF_SOURCE => token}), do: token
  defp event_source_token(_), do: nil

  defp aggregate_results(results, errors) do
    {count, all_errors} =
      for result <- results, reduce: {0, errors} do
        {acc, errs} ->
          case result do
            {:ok, n} -> {acc + n, errs}
            :ok -> {acc, errs}
            {:error, more} when is_list(more) -> {acc, Enum.reverse(more) ++ errs}
            {:error, err} -> {acc, [err | errs]}
          end
      end

    if all_errors == [], do: {:ok, count}, else: {:error, all_errors}
  end

  operation :cloudflare, false

  def cloudflare(conn, %{"batch" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.Raw)
  end

  def cloudflare(conn, log_params) when is_map(log_params) do
    log_params
    |> Map.drop(["source", "timestamp", "id"])
    |> List.wrap()
    |> dispatch(conn, Logs.Raw)
  end

  operation :syslog, false

  def syslog(conn, %{"batch" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.Raw)
  end

  operation :generic_json, false

  def generic_json(conn, %{"_json" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.GenericJson)
  end

  def generic_json(%{body_params: event} = conn, _log_params) do
    event
    |> List.wrap()
    |> dispatch(conn, Logs.GenericJson)
  end

  operation :vector, false

  def vector(conn, %{"_json" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.Vector)
  end

  def vector(%{body_params: event} = conn, _log_params) do
    event
    |> List.wrap()
    |> dispatch(conn, Logs.Vector)
  end

  operation :browser_reports, false

  def browser_reports(conn, %{"_json" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.BrowserReport)
  end

  def browser_reports(%{body_params: event} = conn, _log_params) do
    event
    |> List.wrap()
    |> dispatch(conn, Logs.BrowserReport)
  end

  operation :elixir_logger, false

  def elixir_logger(conn, %{"batch" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.Raw)
  end

  operation :create_with_typecasts, false

  def create_with_typecasts(conn, %{"batch" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.IngestTypecasting)
  end

  operation :vercel_ingest, false

  def vercel_ingest(conn, %{"_json" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.Vercel)
  end

  operation :netlify, false

  def netlify(conn, %{"_json" => batch}) when is_list(batch) do
    batch
    |> dispatch(conn, Logs.Netlify)
  end

  def netlify(%{body_params: params} = conn, _params) when is_map(params) do
    [params]
    |> dispatch(conn, Logs.Netlify)
  end

  operation :github, false

  def github(%{body_params: params} = conn, _params) do
    [params]
    |> dispatch(conn, Logs.Github)
  end

  operation :cloud_event, false

  def cloud_event(%Plug.Conn{} = conn, %{"_json" => batch})
      when is_list(batch) do
    do_cloud_event(conn, batch)
  end

  def cloud_event(%Plug.Conn{body_params: event} = conn, _log_params) do
    do_cloud_event(conn, event)
  end

  defp do_cloud_event(%Plug.Conn{} = conn, data) do
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
    |> dispatch(conn, Logs.Raw)
  end

  defp handle({:ok, _}, conn), do: render(conn, "index.json", message: @message)
  defp handle(:ok, conn), do: render(conn, "index.json", message: @message)

  defp handle({:error, :spool_unavailable}, conn) do
    conn
    |> put_status(500)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: "Internal server error")
  end

  defp handle({:error, :source_not_found}, conn) do
    conn
    |> put_status(404)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: @source_not_found_message)
  end

  defp handle({:error, :source_unavailable}, conn) do
    conn
    |> put_status(503)
    |> put_resp_header("retry-after", @source_unavailable_retry_after)
    |> put_view(LogflareWeb.LogView)
    |> render("index.json", message: @source_unavailable_message)
  end

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

  defp protobuf_response({:error, :source_not_found}, conn, _success_response) do
    send_proto_error(conn, 404, @source_not_found_message)
  end

  defp protobuf_response({:error, :source_unavailable}, conn, _success_response) do
    conn
    |> put_resp_header("retry-after", @source_unavailable_retry_after)
    |> send_proto_error(503, @source_unavailable_message)
  end

  defp protobuf_response({:error, :spool_unavailable}, conn, _success_response) do
    send_proto_error(conn, 500, "Internal server error")
  end

  defp protobuf_response({:error, errors}, conn, success_response) when is_list(errors) do
    source = conn.assigns.source

    Logger.warning("OTLP ingest rejected #{length(errors)} event(s) at validation",
      source_token: source.token,
      source_id: source.id,
      sample_errors: errors |> Enum.uniq() |> Enum.take(3)
    )

    send_proto_resp(conn, success_response)
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
