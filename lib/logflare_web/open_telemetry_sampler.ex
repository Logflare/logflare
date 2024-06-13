defmodule LogflareWeb.OpenTelemetrySampler do
  require OpenTelemetry.Tracer, as: Tracer
  @behaviour :otel_sampler

  @impl :otel_sampler
  def setup(opts) do
    :otel_sampler_trace_id_ratio_based.setup(opts.probability)
  end

  @impl :otel_sampler
  def description(_sampler_config) do
    "LogflareWeb.OpenTelemetrySampler"
  end

  @doc """
  Drops traces for ingest and endpoints related routes.

  Delegates to trace id sampler.

  iex> ctx =  %{}
  iex> trace_id =  75141356756228984281078696925651880580
  iex> links =  {:links, 128, 128, :infinity, 0, []}
  iex> span_name = "HTTP POST"
  iex> span_kind = :server
  iex> attributes = %{ "http.method": "POST", "http.target": "/logs/json"}
  iex> sampler_config = %{probability: 0.001, id_upper_bound: 9223372036854776.0}
  iex> {decision, _, _state} = should_sample(ctx, trace_id, links, span_name, span_kind, attributes, sampler_config)
  iex> decision in [:drop, :record_and_sample]
  true
  """
  @impl :otel_sampler
  def should_sample(
        ctx,
        trace_id,
        links,
        span_name,
        span_kind,
        attributes,
        sampler_config
      ) do
    tracestate = Tracer.current_span_ctx(ctx) |> OpenTelemetry.Span.tracestate()

    exclude_route? =
      case Map.get(attributes, "http.target") do
        "/logs" <> _ -> true
        "/api/logs" <> _ -> true
        "/api/events" <> _ -> true
        "/endpoints/query" <> _ -> true
        "/api/endpoints/query" <> _ -> true
        _ -> false
      end

    if exclude_route? do
      {:drop, [], tracestate}
    else
      :otel_sampler_trace_id_ratio_based.should_sample(
        ctx,
        trace_id,
        links,
        span_name,
        span_kind,
        attributes,
        sampler_config
      )
    end
  end
end
