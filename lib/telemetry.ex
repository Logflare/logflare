defmodule Logflare.Telemetry do
  use Supervisor
  import Telemetry.Metrics

  def start_link(arg), do: Supervisor.start_link(__MODULE__, arg, name: __MODULE__)

  @caches [
    {Logflare.Logs.LogEvents.Cache, :log_events},
    {Logflare.Logs.RejectedLogEvents, :rejected_log_events},
    {Logflare.Sources.Cache, :sources},
    {Logflare.SourceSchemas.Cache, :source_schemas},
    {Logflare.PubSubRates.Cache, :pub_sub_rates},
    {Logflare.Billing.Cache, :billing},
    {Logflare.Users.Cache, :users}
  ]

  @impl true
  def init(_arg) do
    otel_exporter =
      if Application.get_env(:logflare, :opentelemetry_enabled?) do
        otel_exporter_opts =
          Application.get_all_env(:opentelemetry_exporter)
          |> Keyword.put(:metrics, metrics())
          |> Keyword.put(:resource, %{
            name: "Logflare",
            service: %{
              name: "Logflare",
              version: Application.spec(:logflare, :vsn) |> to_string()
            },
            instance: inspect(Node.self())
          })
          |> Keyword.update!(:otlp_headers, &Map.new/1)

        [{OtelMetricExporter, otel_exporter_opts}]
      else
        []
      end

    children =
      [
        {:telemetry_poller, measurements: periodic_measurements(), period: 10_000}
      ] ++ otel_exporter

    Supervisor.init(children, strategy: :one_for_one)
  end

  def metrics do
    cache_stats? = Application.get_env(:logflare, :cache_stats, false)

    cache_metrics =
      if cache_stats? do
        Enum.flat_map(@caches, fn {_cache, metric} ->
          [
            last_value("cachex.#{metric}.purge"),
            last_value("cachex.#{metric}.stats"),
            last_value("cachex.#{metric}.evictions"),
            last_value("cachex.#{metric}.expirations"),
            last_value("cachex.#{metric}.operations"),
            last_value("cachex.#{metric}.total_heap_size", unit: {:byte, :megabyte})
          ]
        end)
      else
        []
      end

    phoenix_metrics = [
      distribution("phoenix.endpoint.stop.duration", unit: {:native, :millisecond}),
      distribution("phoenix.router_dispatch.stop.duration", unit: {:native, :millisecond})
    ]

    database_metrics = [
      distribution("logflare.repo.query.total_time", unit: {:native, :millisecond}),
      # TODO: decode_time is `nil` in some of the ecto queries
      # In most telemetry adapters this is fine, but it causes issues in OtelMetricExporter
      # distribution("logflare.repo.query.decode_time", unit: {:native, :millisecond}),
      distribution("logflare.repo.query.query_time", unit: {:native, :millisecond}),
      distribution("logflare.repo.query.queue_time", unit: {:native, :millisecond}),
      distribution("logflare.repo.query.idle_time", unit: {:native, :millisecond})
    ]

    vm_metrics = [
      last_value("vm.memory.total", unit: {:byte, :kilobyte}),
      last_value("vm.total_run_queue_lengths.total"),
      last_value("vm.total_run_queue_lengths.cpu"),
      last_value("vm.total_run_queue_lengths.io"),
      last_value("logflare.system.observer.metrics.total_active_tasks")
    ]

    broadway_metrics = [
      distribution("broadway.batcher.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.batch_processor.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.processor.message.stop.duration", unit: {:native, :millisecond}),
      distribution("broadway.processor.stop.duration", unit: {:native, :millisecond})
    ]

    application_metrics = [
      distribution("logflare.goth.fetch.stop.duration",
        tags: [:partition],
        unit: {:native, :millisecond}
      ),
      distribution("logflare.logs.processor.ingest.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      counter("logflare.logs.processor.ingest.stop.duration",
        tags: [:processor],
        description: "Ingestion execution counts"
      ),
      distribution("logflare.logs.processor.ingest.logs.count",
        tags: [:processor],
        description: "Ingestion batch size"
      ),
      distribution("logflare.logs.processor.ingest.store.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      distribution("logflare.logs.processor.ingest.handle_batch.stop.duration",
        tags: [:processor],
        unit: {:native, :millisecond}
      ),
      distribution("logflare.ingest.pipeline.handle_batch.batch_size",
        tags: [:pipeline],
        reporter_opts: batch_size_reporter_opts()
      ),
      distribution("logflare.ingest.common_pipeline.handle_batch.batch_size",
        tags: [:pipeline],
        reporter_opts: batch_size_reporter_opts()
      ),
      counter("logflare.context_cache.busted.count", tags: [:schema, :table]),
      counter("logflare.context_cache.handle_record.count", tags: [:schema, :table]),
      counter("logflare.logs.ingest_logs.drop",
        tags: [],
        description: "Ingest drops"
      ),
      counter("logflare.logs.ingest_logs.rejected",
        tags: [],
        description: "Ingest rejects"
      ),
      counter("logflare.logs.ingest_logs.buffer_full",
        tags: [],
        description: "Ingest buffer fulls"
      ),
      counter("logflare.rate_limiter.rejected",
        tags: [],
        description: "Rate limited API hits"
      ),
      last_value("logflare.backends.egress.request_length", tags: [:backend_id]),
      last_value("logflare.system.finch.in_flight_requests", tags: [:pool, :url]),
      last_value("logflare.google.set_iam_policy.members",
        description: "Google IAM policy members count"
      ),
      last_value("logflare.backends.dynamic_pipeline.pipeline_count",
        tags: [:backend_id]
      ),
      last_value("logflare.backends.ingest_event_queue.queue_janitor.length",
        tags: [:backend_id]
      ),
      distribution("logflare.ingest.pipeline.stream_batch.stop.duration",
        tags: [:source_token],
        unit: {:native, :millisecond}
      ),
      last_value("logflare.backends.dynamic_pipeline.increment.success_count",
        tags: [:backend_id],
        description: "Dynamic pipeline sucessfully increment count"
      ),
      last_value("logflare.backends.dynamic_pipeline.increment.error_count",
        tags: [:backend_id],
        description: "Dynamic pipeline failed increment count"
      ),
      last_value("logflare.backends.dynamic_pipeline.decrement.success_count",
        tags: [:backend_id],
        description: "Dynamic pipeline sucessfully decrement count"
      ),
      last_value("logflare.backends.dynamic_pipeline.decrement.error_count",
        tags: [:backend_id],
        description: "Dynamic pipeline failed decrement count"
      ),
      distribution("logflare.endpoints.run_query.exec_query_on_backend.stop.duration",
        tags: [:endpoint_id],
        unit: {:native, :millisecond},
        description: "Endpoint query execution duration"
      ),
      counter("logflare.endpoints.run_query.exec_query_on_backend.stop.duration",
        tags: [:endpoint_id],
        description: "Endpoint query execution counts"
      ),
      distribution("logflare.endpoints.run_query.exec_query_on_backend.total_rows",
        tags: [:endpoint_id],
        description: "Number of rows returned by endpoint query execution"
      )
    ]

    Enum.concat([
      phoenix_metrics,
      database_metrics,
      vm_metrics,
      cache_metrics,
      broadway_metrics,
      application_metrics
    ])
  end

  defp periodic_measurements do
    cache_stats? = Application.get_env(:logflare, :cache_stats, false)

    if cache_stats? do
      [{__MODULE__, :cachex_metrics, []}]
    else
      []
    end
  end

  def cachex_metrics do
    Enum.each(@caches, fn {cache, metric} ->
      {:ok, stats} = Cachex.stats(cache)

      {:total_heap_size, total_heap_size} =
        cache
        |> Process.whereis()
        |> Process.info(:total_heap_size)

      metrics = %{
        purge: Map.get(stats, :purge),
        stats: Map.get(stats, :stats),
        evictions: Map.get(stats, :evictions),
        expirations: Map.get(stats, :expirations),
        operations: Map.get(stats, :operations),
        total_heap_size: total_heap_size
      }

      :telemetry.execute([:cachex, metric], metrics)
    end)
  end

  defp batch_size_reporter_opts do
    [buckets: [0, 1, 5, 10, 50, 100, 150, 250, 500, 1000, 2000]]
  end
end
