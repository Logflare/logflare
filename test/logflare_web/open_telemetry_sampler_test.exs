defmodule LogflareWeb.OpenTelemetrySamplerTest do
  use LogflareWeb.ConnCase

  alias LogflareWeb.OpenTelemetrySampler

  setup do
    prev_ingest = Application.get_env(:logflare, :ingest_sample_ratio)
    prev_endpoint = Application.get_env(:logflare, :endpoint_sample_ratio)
    prev_metadata = Application.get_env(:logflare, :metadata)

    Application.put_env(:logflare, :ingest_sample_ratio, 1.0)
    Application.put_env(:logflare, :endpoint_sample_ratio, 1.0)
    Application.put_env(:logflare, :metadata, [])

    on_exit(fn ->
      Application.put_env(:logflare, :ingest_sample_ratio, prev_ingest)
      Application.put_env(:logflare, :endpoint_sample_ratio, prev_endpoint)
      Application.put_env(:logflare, :metadata, prev_metadata)
    end)

    :ok
  end

  doctest LogflareWeb.OpenTelemetrySampler, import: true

  describe "setup/1" do
    test "delegates to trace id ratio based sampler" do
      opts = %{probability: 0.5}
      assert config = %{probability: 0.5} = OpenTelemetrySampler.setup(opts)

      expected_config = :otel_sampler_trace_id_ratio_based.setup(0.5)
      assert config == expected_config
    end
  end

  describe "description/1" do
    test "returns the module name as description" do
      opts = %{probability: 1}
      config = OpenTelemetrySampler.setup(opts)
      assert OpenTelemetrySampler.description(config) == "LogflareWeb.OpenTelemetrySampler"
    end
  end

  describe "should_sample/7" do
    setup do
      ctx = %{}
      trace_id = :otel_id_generator.generate_trace_id()
      links = {:links, 128, 128, :infinity, 0, []}
      span_name = "HTTP POST"
      span_kind = :server
      sampler_config = :otel_sampler_trace_id_ratio_based.setup(1.0)

      %{
        ctx: ctx,
        trace_id: trace_id,
        links: links,
        span_name: span_name,
        span_kind: span_kind,
        sampler_config: sampler_config
      }
    end

    test "samples ingest routes with ingest sample ratio", %{
      ctx: ctx,
      trace_id: trace_id,
      links: links,
      span_name: span_name,
      span_kind: span_kind,
      sampler_config: sampler_config
    } do
      Application.put_env(:logflare, :ingest_sample_ratio, 0.0)

      for url_path <- ["/logs/json", "/api/logs", "/api/events"] do
        attributes = %{"http.request.method": "POST", "url.path": url_path}

        assert {:drop, _attrs, _state} =
                 OpenTelemetrySampler.should_sample(
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

    test "samples endpoint routes with endpoint sample ratio", %{
      ctx: ctx,
      trace_id: trace_id,
      links: links,
      span_name: span_name,
      span_kind: span_kind,
      sampler_config: sampler_config
    } do
      Application.put_env(:logflare, :endpoint_sample_ratio, 0.0)

      for url_path <- ["/endpoints/query/123", "/api/endpoints/query/456"] do
        attributes = %{"http.request.method": "POST", "url.path": url_path}

        assert {:drop, _attrs, _state} =
                 OpenTelemetrySampler.should_sample(
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

    test "uses default sampler config for other routes", %{
      ctx: ctx,
      trace_id: trace_id,
      links: links,
      span_name: span_name,
      span_kind: span_kind
    } do
      Application.put_env(:logflare, :ingest_sample_ratio, 0.0)
      Application.put_env(:logflare, :endpoint_sample_ratio, 0.0)

      sampler_config = OpenTelemetrySampler.setup(%{probability: 1})
      attributes = %{"http.request.method": "GET", "url.path": "/dashboard"}

      {decision, _attrs, _state} =
        OpenTelemetrySampler.should_sample(
          ctx,
          trace_id,
          links,
          span_name,
          span_kind,
          attributes,
          sampler_config
        )

      assert decision != :drop
    end

    test "redacts api_key in url.query", %{
      ctx: ctx,
      trace_id: trace_id,
      links: links,
      span_name: span_name,
      span_kind: span_kind,
      sampler_config: sampler_config
    } do
      attributes = %{
        "http.request.method": "POST",
        "url.path": "/logs",
        "url.query": "api_key=secret123&other=value"
      }

      {_decision, attrs, _state} =
        OpenTelemetrySampler.should_sample(
          ctx,
          trace_id,
          links,
          span_name,
          span_kind,
          attributes,
          sampler_config
        )

      url_query = Keyword.get(attrs, :"url.query")
      assert url_query == "api_key=[REDACTED]&other=value"

      attributes = %{
        "http.request.method": "POST",
        "url.path": "/logs",
        "url.query": "api_key=secret123&param=value&api_key=another_secret"
      }

      {_decision, attrs, _state} =
        OpenTelemetrySampler.should_sample(
          ctx,
          trace_id,
          links,
          span_name,
          span_kind,
          attributes,
          sampler_config
        )

      url_query = Keyword.get(attrs, :"url.query")
      assert url_query == "api_key=[REDACTED]&param=value&api_key=[REDACTED]"
    end

    test "handles missing and nil url.query gracefully", %{
      ctx: ctx,
      trace_id: trace_id,
      links: links,
      span_name: span_name,
      span_kind: span_kind,
      sampler_config: sampler_config
    } do
      attributes = %{"http.request.method": "POST", "url.path": "/logs"}

      {_decision, attrs, _state} =
        OpenTelemetrySampler.should_sample(
          ctx,
          trace_id,
          links,
          span_name,
          span_kind,
          attributes,
          sampler_config
        )

      refute Keyword.has_key?(attrs, :"url.query")

      attributes = %{
        "http.request.method": "POST",
        "url.path": "/logs",
        "url.query": nil
      }

      {_decision, attrs, _state} =
        OpenTelemetrySampler.should_sample(
          ctx,
          trace_id,
          links,
          span_name,
          span_kind,
          attributes,
          sampler_config
        )

      refute Keyword.has_key?(attrs, :"url.query")
    end

    test "adds server metadata as attributes", %{
      ctx: ctx,
      trace_id: trace_id,
      links: links,
      span_name: span_name,
      span_kind: span_kind,
      sampler_config: sampler_config
    } do
      Application.put_env(:logflare, :metadata,
        version: "1.0.0",
        environment: "test",
        nil_value: nil
      )

      attributes = %{"http.request.method": "GET", "url.path": "/dashboard"}

      {_decision, attrs, _state} =
        OpenTelemetrySampler.should_sample(
          ctx,
          trace_id,
          links,
          span_name,
          span_kind,
          attributes,
          sampler_config
        )

      assert Keyword.get(attrs, :"server.version") == "1.0.0"
      assert Keyword.get(attrs, :"server.environment") == "test"

      # filters out nil values
      refute Keyword.has_key?(attrs, :"server.nil_value")
    end
  end
end
