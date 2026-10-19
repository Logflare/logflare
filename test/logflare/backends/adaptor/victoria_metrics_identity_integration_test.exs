defmodule Logflare.Backends.Adaptor.VictoriaMetricsIdentityIntegrationTest do
  use Logflare.DataCase, async: false

  alias Logflare.Backends.SourceSup
  alias Logflare.Google.BigQuery
  alias Logflare.Logs.OtelMetric
  alias Logflare.Logs.Processor
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Utils.SSRF
  alias Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Common.V1.KeyValueList
  alias Opentelemetry.Proto.Metrics.V1.Gauge
  alias Opentelemetry.Proto.Metrics.V1.Metric
  alias Opentelemetry.Proto.Metrics.V1.NumberDataPoint
  alias Opentelemetry.Proto.Metrics.V1.ResourceMetrics
  alias Opentelemetry.Proto.Metrics.V1.ScopeMetrics
  alias Opentelemetry.Proto.Resource.V1.Resource

  @receiver_url System.get_env("VICTORIA_METRICS_URL", "http://127.0.0.1:8428")
  @receiver_port URI.parse(@receiver_url).port
  @retry [sleep: 250, duration: 30_000]

  setup do
    stub(BigQuery, :get_table, fn _ -> {:error, :not_found} end)

    stub(SSRF, :safe_resolve, fn
      "victoriametrics.test" -> {:ok, {127, 0, 0, 1}}
      host -> Mimic.call_original(SSRF, :safe_resolve, [host])
    end)

    stub(Finch, :build, fn method, url, headers, body ->
      Mimic.call_original(Finch, :build, [method, url, headers, body])
    end)

    stub(Finch, :request, fn
      %Finch.Request{host: "127.0.0.1", port: @receiver_port} = request, pool, opts ->
        Mimic.call_original(Finch, :request, [request, pool, opts])

      _request, _pool, _opts ->
        {:error, :unexpected_external_request}
    end)

    start_supervised!(AllLogsLogged)
    insert(:plan)
    user = insert(:user)
    source = insert(:source, user: user)

    backend =
      insert(:backend,
        type: :victoria_metrics,
        user: user,
        sources: [source],
        config: %{
          url: "http://victoriametrics.test:#{@receiver_port}/api/v1/write",
          labels: %{"env" => "integration"}
        }
      )

    start_supervised!({SourceSup, source})

    prefix = "vm_query_" <> String.replace(Ecto.UUID.generate(), "-", "")
    %{source: source, backend: backend, prefix: prefix, user: user}
  end

  test "keeps resource and scope variants separate at the same timestamp", %{
    source: source,
    prefix: prefix
  } do
    timestamp = System.system_time(:second) - 120
    metric_name = prefix <> "_identity"

    context = [
      service_name: prefix,
      scope_schema_url: "https://opentelemetry.io/schemas/1.31.0",
      resource_attributes: [
        attribute("host.name", "host-a"),
        attribute("deployment", %{"role" => "api", "zones" => ["one", "two"]}),
        attribute("locations", [%{"region" => "eu"}])
      ],
      scope_attributes: [attribute("build.flags", ["alpha", "beta"])]
    ]

    variants = [
      {10.0, []},
      {20.0, instance: "replica-b", resource_attributes: [attribute("host.name", "host-b")]},
      {30.0, scope_name: "vm.query.other"},
      {40.0, scope_version: "2.0"},
      {50.0, scope_attributes: [attribute("build.flags", ["alpha", "gamma"])]},
      {60.0, scope_attributes: [attribute("build.flags", ["beta", "alpha"])]},
      {70.0,
       scope_attributes: [
         attribute("build.config", %{"mode" => "alpha", "flags" => ["read"]}),
         attribute("build.locations", [%{"region" => "eu"}])
       ]},
      {80.0,
       scope_attributes: [
         attribute("build.config", %{"mode" => "beta", "flags" => ["read"]}),
         attribute("build.locations", [%{"region" => "us"}])
       ]}
    ]

    resource_metrics =
      Enum.map(variants, fn {value, overrides} ->
        metric_data(
          metric_name,
          [{timestamp, value}],
          Keyword.merge(context, overrides)
        )
      end)

    request =
      %ExportMetricsServiceRequest{resource_metrics: resource_metrics}
      |> ExportMetricsServiceRequest.encode()
      |> IO.iodata_to_binary()
      |> ExportMetricsServiceRequest.decode()

    assert {:ok, _count} = Processor.ingest(request.resource_metrics, OtelMetric, source)

    TestUtils.retry_assert(@retry, fn ->
      assert {:ok,
              %{
                "status" => "success",
                "data" => %{"resultType" => "vector", "result" => results}
              }} =
               query_receiver(metric_name, %{"time" => timestamp})

      assert length(results) == length(variants)

      by_value =
        Map.new(results, fn %{"metric" => labels, "value" => [sample_time, value]} ->
          assert sample_time == timestamp
          assert labels["__name__"] == metric_name
          assert labels["source"] == source.name
          assert labels["job"] == "integration/" <> prefix
          assert labels["env"] == "integration"
          assert labels["_http_route"] == "/metrics"
          refute Map.has_key?(labels, "logflare_resource_id")
          refute Map.has_key?(labels, "logflare_scope_id")
          refute Map.has_key?(labels, "_host_name")
          assert labels["otel_scope_schema_url"] == "https://opentelemetry.io/schemas/1.31.0"
          {value, labels}
        end)

      assert Enum.sort(Map.keys(by_value)) == ~w(10 20 30 40 50 60 70 80)
      assert by_value["10"]["instance"] == "replica-a"
      assert by_value["20"]["instance"] == "replica-b"
      assert by_value["30"]["otel_scope_name"] == "vm.query.other"
      assert by_value["40"]["otel_scope_version"] == "2.0"
      assert by_value["10"]["otel_scope_build_flags"] == ~s(["alpha","beta"])
      assert by_value["50"]["otel_scope_build_flags"] == ~s(["alpha","gamma"])
      assert by_value["60"]["otel_scope_build_flags"] == ~s(["beta","alpha"])
      assert by_value["70"]["otel_scope_build_config"] == ~s({"flags":["read"],"mode":"alpha"})
      assert by_value["80"]["otel_scope_build_config"] == ~s({"flags":["read"],"mode":"beta"})
      assert by_value["70"]["otel_scope_build_locations"] == ~s([{"region":"eu"}])
      assert by_value["80"]["otel_scope_build_locations"] == ~s([{"region":"us"}])

      for value <- ~w(10 20 50 60 70 80) do
        assert by_value[value]["otel_scope_name"] == "vm.query.integration"
        assert by_value[value]["otel_scope_version"] == "1.0"
      end

      assert {:ok, %{"data" => %{"result" => targets}}} =
               query_receiver(~s(target_info{job="integration/#{prefix}"}), %{"time" => timestamp})

      assert length(targets) == 2

      assert Map.new(targets, fn %{"metric" => labels, "value" => [^timestamp, "1"]} ->
               refute Map.has_key?(labels, "source")
               refute Map.has_key?(labels, "env")
               refute Map.has_key?(labels, "otel_scope_name")
               {labels["instance"], labels["_host_name"]}
             end) == %{"replica-a" => "host-a", "replica-b" => "host-b"}

      assert [%{"metric" => metadata}] =
               Enum.filter(targets, &(&1["metric"]["instance"] == "replica-a"))

      assert metadata["deployment"] == ~s({"role":"api","zones":["one","two"]})
      assert metadata["locations"] == ~s([{"region":"eu"}])
    end)
  end

  @spec query_receiver(String.t(), map()) :: {:ok, term()} | {:error, Jason.DecodeError.t()}
  defp query_receiver(query, params) do
    encoded = URI.encode_query(Map.put(params, "query", query))
    url = @receiver_url <> "/api/v1/query?" <> encoded
    {:ok, %{status_code: 200, body: body}} = HTTPoison.get(url)
    Jason.decode(body)
  end

  @spec metric_data(String.t(), [{integer(), float()}], keyword()) :: ResourceMetrics.t()
  defp metric_data(name, samples, context) do
    %ResourceMetrics{
      resource: %Resource{
        attributes:
          [
            attribute("service.name", Keyword.get(context, :service_name, "metrics-api")),
            attribute("service.namespace", "integration"),
            attribute("service.instance.id", Keyword.get(context, :instance, "replica-a"))
          ] ++ Keyword.get(context, :resource_attributes, [])
      },
      scope_metrics: [
        %ScopeMetrics{
          schema_url: Keyword.get(context, :scope_schema_url, ""),
          scope: %InstrumentationScope{
            name: Keyword.get(context, :scope_name, "vm.query.integration"),
            version: Keyword.get(context, :scope_version, "1.0"),
            attributes: Keyword.get(context, :scope_attributes, [])
          },
          metrics: [
            %Metric{
              name: name,
              data:
                {:gauge,
                 %Gauge{
                   data_points:
                     for {timestamp, value} <- samples do
                       %NumberDataPoint{
                         time_unix_nano: timestamp * 1_000_000_000,
                         value: {:as_double, value},
                         attributes: [
                           attribute("env", "event"),
                           attribute("http.route", "/metrics")
                         ]
                       }
                     end
                 }}
            }
          ]
        }
      ]
    }
  end

  @spec attribute(String.t(), String.t() | list() | map()) :: KeyValue.t()
  defp attribute(key, value), do: %KeyValue{key: key, value: attribute_value(value)}

  @spec attribute_value(String.t() | list() | map()) :: AnyValue.t()
  defp attribute_value(values) when is_list(values) do
    %AnyValue{value: {:array_value, %ArrayValue{values: Enum.map(values, &attribute_value/1)}}}
  end

  defp attribute_value(values) when is_map(values) do
    entries = Enum.map(values, fn {key, value} -> attribute(key, value) end)
    %AnyValue{value: {:kvlist_value, %KeyValueList{values: entries}}}
  end

  defp attribute_value(value) when is_binary(value), do: %AnyValue{value: {:string_value, value}}
end
