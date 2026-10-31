defmodule Logflare.Backends.Adaptor.TelemetryConfigurationTest do
  use ExUnit.Case, async: true

  import Mimic

  alias Logflare.Backends.Adaptor
  alias Logflare.Backends.Adaptor.BigQueryAdaptor
  alias Logflare.Backends.Adaptor.ClickHouseAdaptor
  alias Logflare.Backends.Adaptor.DatadogAdaptor
  alias Logflare.Backends.Adaptor.HttpBased
  alias Logflare.Backends.Adaptor.IncidentioAdaptor
  alias Logflare.Backends.Adaptor.LokiAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.S3Adaptor
  alias Logflare.Backends.Adaptor.SyslogAdaptor
  alias Logflare.Backends.Adaptor.WebhookAdaptor
  alias Logflare.Backends.Backend
  alias Logflare.Sources.Source

  @adaptors Application.spec(:logflare, :modules)
            |> Enum.filter(fn module ->
              Code.ensure_loaded?(module) and
                Adaptor in List.flatten(
                  Keyword.get_values(module.__info__(:attributes), :behaviour)
                )
            end)
            |> Enum.sort()
  @backend_mapping Backend.adaptor_mapping()

  setup :verify_on_exit!

  test "every compiled Adaptor implementation is registered as a backend" do
    assert MapSet.size(MapSet.new(@adaptors)) > 0
    assert MapSet.new(@adaptors) == MapSet.new(Map.values(@backend_mapping))
  end

  for adaptor <- @adaptors,
      {backend_type, registered_adaptor} <- @backend_mapping,
      registered_adaptor == adaptor,
      transport <- if(backend_type == :elastic, do: ~w(otlp filebeat logstash), else: [nil]) do
    test "#{inspect(adaptor)} configures backend tags for #{backend_type} #{transport}" do
      backend_type = unquote(backend_type)
      source = %Source{id: 101, user_id: 201}
      config = Map.put(backend_config(backend_type), :transport, unquote(transport))
      backend = %Backend{id: 301, type: backend_type, config: config}
      test_pid = self()
      ref = make_ref()

      stub(WebhookAdaptor, :start_link, fn {source, backend} ->
        WebhookAdaptor.Pipeline.start_link(%{
          source: source,
          backend: backend,
          config: backend.config
        })
      end)

      expect(Broadway, :start_link, fn pipeline, opts ->
        send(test_pid, {ref, pipeline, Keyword.fetch!(opts, :context)})
        {:ok, test_pid}
      end)

      assert {:ok, ^test_pid} = start_pipeline(unquote(adaptor), source, backend)
      assert_receive {^ref, pipeline, context}
      assert function_exported?(pipeline, :handle_message, 3)
      assert Map.fetch!(context, :telemetry_tags) == %{backend_type: backend_type}
    end
  end

  @spec backend_config(atom()) :: map()
  defp backend_config(:datadog), do: %{region: "US1", api_key: "test-api-key"}
  defp backend_config(_type), do: %{url: "http://localhost"}

  @spec start_pipeline(module(), Source.t(), Backend.t()) :: Broadway.on_start()
  defp start_pipeline(ClickHouseAdaptor, _source, backend) do
    ClickHouseAdaptor.Pipeline.start_link(name: :telemetry_conformance, backend: backend)
  end

  defp start_pipeline(PostgresAdaptor, source, backend) do
    PostgresAdaptor.Pipeline.start_link(%PostgresAdaptor{
      source: source,
      backend: backend,
      pipeline_name: :telemetry_conformance
    })
  end

  defp start_pipeline(S3Adaptor, source, backend) do
    S3Adaptor.Pipeline.start_link(
      pipeline_name: :telemetry_conformance,
      source_id: source.id,
      backend_id: backend.id,
      batch_timeout: 1_000
    )
  end

  defp start_pipeline(SyslogAdaptor, source, backend) do
    SyslogAdaptor.Pipeline.start_link(
      name: :telemetry_conformance,
      source: source,
      backend: backend,
      pool: :telemetry_conformance_pool
    )
  end

  defp start_pipeline(BigQueryAdaptor, source, backend) do
    Logflare.Sources.Source.BigQuery.Pipeline.start_link(
      name: :telemetry_conformance,
      source: source,
      backend: backend,
      pipeline_ref: make_ref()
    )
  end

  defp start_pipeline(adaptor, source, backend)
       when adaptor in [WebhookAdaptor, DatadogAdaptor, LokiAdaptor, IncidentioAdaptor] do
    adaptor.start_link({source, backend})
  end

  defp start_pipeline(adaptor, source, backend) do
    assert HttpBased.Client in List.flatten(
             Keyword.get_values(adaptor.__info__(:attributes), :behaviour)
           ),
           "add a pipeline startup fixture for #{inspect(adaptor)}"

    adaptor.start_link({source, backend})
  end
end
