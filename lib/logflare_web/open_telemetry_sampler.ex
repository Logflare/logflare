defmodule LogflareWeb.OpenTelemetrySampler do
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

  ingestion routes can be sampled separately from the main sample ratio
  iex> ctx =  %{}
  iex> trace_id =  :otel_id_generator.generate_trace_id()
  iex> links =  {:links, 128, 128, :infinity, 0, []}
  iex> span_name = "HTTP POST"
  iex> span_kind = :server
  iex> attributes = %{ "http.request.method": "POST", "url.path": "/logs/json"}
  iex> sampler_config = :otel_sampler_trace_id_ratio_based.setup(1.0)
  iex> Application.put_env(:logflare, :ingest_sample_ratio, 0.0)
  iex> {decision, _, _state} = should_sample(ctx, trace_id, links, span_name, span_kind, attributes, sampler_config)
  iex> decision == :drop
  true
  iex> Application.put_env(:logflare, :ingest_sample_ratio, 1.0)
  iex> {decision, _, _state} = should_sample(ctx, trace_id, links, span_name, span_kind, attributes, sampler_config)
  iex> decision == :drop
  false

  endpoint routes routes can be sampled separately from the main sample ratio
  iex> ctx =  %{}
  iex> trace_id =  :otel_id_generator.generate_trace_id()
  iex> links =  {:links, 128, 128, :infinity, 0, []}
  iex> span_name = "HTTP POST"
  iex> span_kind = :server
  iex> attributes = %{ "http.request.method": "POST", "url.path": "/api/endpoints/query/123"}
  iex> sampler_config = :otel_sampler_trace_id_ratio_based.setup(1.0)
  iex> Application.put_env(:logflare, :endpoint_sample_ratio, 0.0)
  iex> {decision, _, _state} = should_sample(ctx, trace_id, links, span_name, span_kind, attributes, sampler_config)
  iex> decision == :drop
  true
  iex> Application.put_env(:logflare, :endpoint_sample_ratio, 1.0)
  iex> {decision, _, _state} = should_sample(ctx, trace_id, links, span_name, span_kind, attributes, sampler_config)
  iex> decision == :drop
  false
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
    config =
      case Map.get(attributes, :"url.path") do
        "/logs" <> _ -> ingest_config()
        "/api/logs" <> _ -> ingest_config()
        "/api/events" <> _ -> ingest_config()
        "/endpoints/query" <> _ -> endpoint_config()
        "/api/endpoints/query" <> _ -> endpoint_config()
        _ -> sampler_config
      end

    {decision, _attrs, tracestate} =
      :otel_sampler_trace_id_ratio_based.should_sample(
        ctx,
        trace_id,
        links,
        span_name,
        span_kind,
        attributes,
        config
      )

    url_query = Map.get(attributes, :"url.query", "") || ""

    extra_attrs =
      for {k, v} <- Application.get_env(:logflare, :metadata, []), v != nil do
        {String.to_atom("server.#{k}"), v}
      end

    if url_query =~ "api_key=" do
      replaced = url_query |> String.replace(~r/api_key=[^&]*/, "api_key=[REDACTED]")

      {decision,
       [
         {:"url.query", replaced}
       ] ++ extra_attrs, tracestate}
    else
      {decision, extra_attrs, tracestate}
    end
  end

  defp ingest_config do
    prob = Application.get_env(:logflare, :ingest_sample_ratio)
    :otel_sampler_trace_id_ratio_based.setup(prob)
  end

  defp endpoint_config do
    prob = Application.get_env(:logflare, :endpoint_sample_ratio)
    :otel_sampler_trace_id_ratio_based.setup(prob)
  end
end
