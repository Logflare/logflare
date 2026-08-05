defmodule Logflare.Backends.BroadwayContextTest do
  use ExUnit.Case, async: false

  import Logflare.Factory
  import Mimic

  alias Logflare.Backends.Adaptor.ClickHouseAdaptor.Pipeline, as: ClickHousePipeline
  alias Logflare.Backends.Adaptor.HttpBased.Pipeline, as: HttpPipeline
  alias Logflare.Backends.Adaptor.PostgresAdaptor
  alias Logflare.Backends.Adaptor.PostgresAdaptor.Pipeline, as: PostgresPipeline
  alias Logflare.Backends.Adaptor.S3Adaptor.Pipeline, as: S3Pipeline
  alias Logflare.Backends.Adaptor.SyslogAdaptor.Pipeline, as: SyslogPipeline
  alias Logflare.Backends.Adaptor.WebhookAdaptor.Pipeline, as: WebhookPipeline
  alias Logflare.Backends.Spool.ConsumerPipeline
  alias Logflare.Backends.Spool.ProducerPipeline
  alias Logflare.Backends.UserMonitoring.IngestPipeline, as: UserMonitoringPipeline
  alias Logflare.Sources.Source.BigQuery.Pipeline, as: BigQueryPipeline

  defmodule QueueStub do
    def resolve(name), do: {:ok, name}
  end

  defmodule StorageStub do
  end

  setup :verify_on_exit!

  setup do
    original_spool_config = Application.get_env(:logflare, :spool)

    Application.put_env(:logflare, :spool,
      bucket: "test-bucket",
      queue_name: "test-queue",
      queue_mod: QueueStub,
      storage_mod: StorageStub
    )

    on_exit(fn ->
      if original_spool_config do
        Application.put_env(:logflare, :spool, original_spool_config)
      else
        Application.delete_env(:logflare, :spool)
      end
    end)

    :ok
  end

  test "Broadway pipelines expose their effective backend type to duration metrics" do
    test_pid = self()

    stub(Broadway, :start_link, fn _pipeline, opts ->
      send(test_pid, {:broadway_context, Keyword.fetch!(opts, :context)})
      {:ok, self()}
    end)

    tag_values = broadway_tag_values()
    source = build(:source, id: 101, user_id: 201)

    clickhouse = build(:backend, id: 301, type: :clickhouse)
    bigquery = build(:backend, id: 302, type: :bigquery)
    postgres = build(:backend, id: 303, type: :postgres)
    axiom = build(:backend, id: 304, type: :axiom)
    s3 = build(:backend, id: 305, type: :s3)
    syslog = build(:backend, id: 306, type: :syslog)
    datadog = build(:backend, id: 307, type: :datadog)

    assert_backend_type(:clickhouse, tag_values, fn ->
      ClickHousePipeline.start_link(name: :clickhouse_context_test, backend: clickhouse)
    end)

    assert_backend_type(:bigquery, tag_values, fn ->
      BigQueryPipeline.start_link(
        name: :bigquery_context_test,
        source: source,
        backend: bigquery,
        pipeline_ref: make_ref()
      )
    end)

    assert_backend_type(:postgres, tag_values, fn ->
      PostgresPipeline.start_link(%PostgresAdaptor{
        source: source,
        backend: postgres,
        pipeline_name: :postgres_context_test
      })
    end)

    assert_backend_type(:axiom, tag_values, fn ->
      HttpPipeline.start_link(source, axiom, __MODULE__)
    end)

    assert_backend_type(:s3, tag_values, fn ->
      S3Pipeline.start_link(
        pipeline_name: :s3_context_test,
        source_id: source.id,
        backend_id: s3.id,
        batch_timeout: 1_000
      )
    end)

    assert_backend_type(:syslog, tag_values, fn ->
      SyslogPipeline.start_link(
        name: :syslog_context_test,
        source: source,
        backend: syslog,
        pool: :test_pool
      )
    end)

    assert_backend_type(:datadog, tag_values, fn ->
      WebhookPipeline.start_link(%{source: source, backend: datadog, config: %{}})
    end)

    assert_backend_type(:webhook, tag_values, fn ->
      WebhookPipeline.start_link(%{source: source, backend: nil, config: %{}})
    end)

    assert_backend_type(:spool_consumer, tag_values, fn ->
      ConsumerPipeline.start_link(name: :spool_consumer_context_test)
    end)

    assert_backend_type(:spool_producer, tag_values, fn ->
      ProducerPipeline.start_link(name: :spool_producer_context_test)
    end)

    assert_backend_type(:user_monitoring, tag_values, fn ->
      UserMonitoringPipeline.start_link(metric_store_name: :user_monitoring_context_test)
    end)
  end

  defp assert_backend_type(expected, tag_values, start_pipeline) do
    assert {:ok, _pid} = start_pipeline.()
    assert_receive {:broadway_context, context}
    assert tag_values.(%{context: context}) == %{backend_type: expected}
  end

  defp broadway_tag_values do
    Logflare.Telemetry.metrics()
    |> Enum.find(&(&1.name == [:broadway, :batch_processor, :stop, :duration]))
    |> Map.fetch!(:tag_values)
  end
end
