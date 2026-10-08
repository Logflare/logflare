defmodule Logflare.Backends.Adaptor.VictoriaMetricsQueryIntegrationTest do
  use LogflareWeb.ConnCase, async: false

  alias Logflare.Backends
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor
  alias Logflare.Backends.Adaptor.VictoriaMetricsAdaptor.SeriesIdentity
  alias Logflare.Backends.AdaptorSupervisor
  alias Logflare.LogEvent
  alias Logflare.Logs.OtelMetric
  alias Logflare.Sources.Source
  alias Logflare.SystemMetrics.AllLogsLogged
  alias Logflare.Utils.SSRF
  alias Opentelemetry.Proto.Common.V1.AnyValue
  alias Opentelemetry.Proto.Common.V1.ArrayValue
  alias Opentelemetry.Proto.Common.V1.InstrumentationScope
  alias Opentelemetry.Proto.Common.V1.KeyValue
  alias Opentelemetry.Proto.Metrics.V1.Gauge
  alias Opentelemetry.Proto.Metrics.V1.Metric
  alias Opentelemetry.Proto.Metrics.V1.NumberDataPoint
  alias Opentelemetry.Proto.Metrics.V1.ResourceMetrics
  alias Opentelemetry.Proto.Metrics.V1.ScopeMetrics
  alias Opentelemetry.Proto.Resource.V1.Resource

  @moduletag :integration

  @receiver_url "http://victoriametrics.test:8428"
  @retry [sleep: 250, duration: 30_000]

  setup do
    stub(SSRF, :safe_resolve, fn
      "victoriametrics.test" -> {:ok, {127, 0, 0, 1}}
      host -> Mimic.call_original(SSRF, :safe_resolve, [host])
    end)

    stub(Finch, :build, fn method, url, headers, body ->
      Mimic.call_original(Finch, :build, [method, url, headers, body])
    end)

    stub(Finch, :request, fn
      %Finch.Request{host: "127.0.0.1", port: 8428} = request, pool, opts ->
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
          url: @receiver_url <> "/api/v1/write",
          query_url: @receiver_url,
          labels: %{"env" => "integration"}
        }
      )

    start_supervised!({AdaptorSupervisor, {source, backend}})

    prefix = "vm_query_" <> String.replace(Ecto.UUID.generate(), "-", "")
    %{source: source, backend: backend, prefix: prefix, user: user}
  end

  test "queries ingested OTEL metrics as native instant and range results", %{
    source: source,
    backend: backend,
    prefix: prefix,
    user: user,
    conn: conn
  } do
    start = div(System.system_time(:second), 10) * 10 - 120
    finish = start + 20
    samples = [{start, 10.0}, {start + 10, 20.0}, {finish, 30.0}]
    metric_name = prefix <> ".temperature"
    query_name = prefix <> "_temperature"

    events = metric_events(source, metric_name, samples)

    assert Enum.map(events, & &1.body["timestamp"]) ==
             Enum.map(samples, &(elem(&1, 0) * 1_000_000))

    assert {:ok, _count} = Backends.ingest_logs(events, source)

    expected_labels =
      events
      |> hd()
      |> Map.fetch!(:body)
      |> SeriesIdentity.labels()
      |> Map.take(["logflare_resource_id", "logflare_scope_id"])
      |> Map.merge(%{
        "__name__" => query_name,
        "source" => source.name,
        "job" => "integration/metrics-api",
        "instance" => "replica-a",
        "env" => "integration",
        "_http_route" => "/metrics",
        "otel_scope_name" => "vm.query.integration",
        "otel_scope_version" => "1.0"
      })

    TestUtils.retry_assert(@retry, fn ->
      assert {:ok,
              %{
                "status" => "success",
                "data" => %{"resultType" => "vector", "result" => [result]}
              }} = VictoriaMetricsAdaptor.execute_promql(backend, query_name, %{"time" => finish})

      assert result["metric"] == expected_labels
      assert result["value"] == [finish, "30"]
    end)

    conn = add_access_token(conn, user, ~w(private))

    instant =
      conn
      |> get(~p"/api/query", %{promql: query_name, backend_id: backend.id, time: finish})
      |> json_response(200)

    assert %{
             "status" => "success",
             "data" => %{"resultType" => "vector", "result" => [instant_result]}
           } = instant

    assert instant_result == %{"metric" => expected_labels, "value" => [finish, "30"]}

    range =
      conn
      |> get(~p"/api/query", %{
        promql: query_name,
        backend_id: backend.id,
        start: start,
        end: finish,
        step: "10s"
      })
      |> json_response(200)

    assert %{
             "status" => "success",
             "data" => %{"resultType" => "matrix", "result" => [range_result]}
           } = range

    assert range_result == %{
             "metric" => expected_labels,
             "values" => [[start, "10"], [start + 10, "20"], [finish, "30"]]
           }
  end

  test "keeps resource and scope variants separate at the same timestamp", %{
    source: source,
    backend: backend,
    prefix: prefix
  } do
    timestamp = System.system_time(:second) - 120
    metric_name = prefix <> "_identity"

    context = [
      resource_attributes: [attribute("host.name", "host-a")],
      scope_attributes: [attribute("build.flags", ["alpha", "beta"])]
    ]

    variants = [
      {10.0, []},
      {20.0, resource_attributes: [attribute("host.name", "host-b")]},
      {30.0, scope_name: "vm.query.other"},
      {40.0, scope_version: "2.0"},
      {50.0, scope_attributes: [attribute("build.flags", ["alpha", "gamma"])]},
      {60.0, scope_attributes: [attribute("build.flags", ["beta", "alpha"])]}
    ]

    events =
      Enum.flat_map(variants, fn {value, overrides} ->
        metric_events(
          source,
          metric_name,
          [{timestamp, value}],
          Keyword.merge(context, overrides)
        )
      end)

    assert {:ok, _count} = Backends.ingest_logs(events, source)

    TestUtils.retry_assert(@retry, fn ->
      assert {:ok,
              %{
                "status" => "success",
                "data" => %{"resultType" => "vector", "result" => results}
              }} =
               VictoriaMetricsAdaptor.execute_promql(backend, metric_name, %{"time" => timestamp})

      assert length(results) == length(variants)

      by_value =
        Map.new(results, fn %{"metric" => labels, "value" => [sample_time, value]} ->
          assert sample_time == timestamp
          assert labels["__name__"] == metric_name
          assert labels["source"] == source.name
          assert labels["job"] == "integration/metrics-api"
          assert labels["instance"] == "replica-a"
          assert labels["env"] == "integration"
          assert labels["_http_route"] == "/metrics"
          assert labels["logflare_resource_id"] =~ ~r/\A[0-9a-f]{64}\z/
          assert labels["logflare_scope_id"] =~ ~r/\A[0-9a-f]{64}\z/
          {value, labels}
        end)

      assert Enum.sort(Map.keys(by_value)) == ~w(10 20 30 40 50 60)
      baseline = by_value["10"]

      assert by_value["20"]["logflare_resource_id"] != baseline["logflare_resource_id"]
      assert by_value["20"]["logflare_scope_id"] == baseline["logflare_scope_id"]
      assert by_value["30"]["otel_scope_name"] == "vm.query.other"
      assert by_value["40"]["otel_scope_version"] == "2.0"

      scope_variants = Enum.map(~w(10 30 40 50 60), &Map.fetch!(by_value, &1))

      assert scope_variants |> Enum.map(& &1["logflare_scope_id"]) |> Enum.uniq() |> length() == 5

      assert Enum.all?(scope_variants, fn labels ->
               labels["logflare_resource_id"] == baseline["logflare_resource_id"]
             end)

      for value <- ~w(10 20 50 60) do
        assert by_value[value]["otel_scope_name"] == "vm.query.integration"
        assert by_value[value]["otel_scope_version"] == "1.0"
      end
    end)
  end

  test "returns native errors from the real PromQL parser", %{
    backend: backend,
    user: user,
    conn: conn
  } do
    response =
      conn
      |> add_access_token(user, ~w(private))
      |> get(~p"/api/query", %{promql: "{", backend_id: backend.id})
      |> json_response(422)

    assert %{"status" => "error", "errorType" => error_type, "error" => message} = response
    assert is_binary(error_type)
    assert is_binary(message)
    assert message != ""
  end

  test "requires API authentication before querying the receiver", %{backend: backend, conn: conn} do
    conn = get(conn, ~p"/api/query", %{promql: "up", backend_id: backend.id})
    assert conn.status == 401
  end

  @spec metric_events(Source.t(), String.t(), [{integer(), float()}], keyword()) :: [LogEvent.t()]
  defp metric_events(source, name, samples, context \\ []) do
    resource_metrics = %ResourceMetrics{
      resource: %Resource{
        attributes:
          [
            attribute("service.name", "metrics-api"),
            attribute("service.namespace", "integration"),
            attribute("service.instance.id", "replica-a")
          ] ++ Keyword.get(context, :resource_attributes, [])
      },
      scope_metrics: [
        %ScopeMetrics{
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

    [resource_metrics]
    |> OtelMetric.handle_batch(source)
    |> Enum.map(&LogEvent.make(&1, %{source: source}))
  end

  @spec attribute(String.t(), String.t() | [String.t()]) :: KeyValue.t()
  defp attribute(key, values) when is_list(values) do
    array = %ArrayValue{values: Enum.map(values, &%AnyValue{value: {:string_value, &1}})}
    %KeyValue{key: key, value: %AnyValue{value: {:array_value, array}}}
  end

  defp attribute(key, value) when is_binary(value),
    do: %KeyValue{key: key, value: %AnyValue{value: {:string_value, value}}}
end
